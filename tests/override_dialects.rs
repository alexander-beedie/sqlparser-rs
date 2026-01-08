// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Comprehensive set of tests for `DialectOverrides` functionality
use sqlparser::ast::*;
use sqlparser::dialect::{
    AnsiDialect, BigQueryDialect, ClickHouseDialect, DatabricksDialect, Dialect, DialectOverrides,
    DuckDbDialect, GenericDialect, HiveDialect, MsSqlDialect, MySqlDialect, PostgreSqlDialect,
    RedshiftSqlDialect, SQLiteDialect, SnowflakeDialect,
};
use sqlparser::parser::Parser;

/// Helper macro to test that overriding a dialect feature works correctly
macro_rules! test_override {
    ($dialect:expr, $method:ident, $default:expr, $override_val:expr) => {{
        // Check the default value
        assert_eq!(
            Dialect::$method(&$dialect),
            $default,
            "Default value for {} should be {}",
            stringify!($method),
            $default
        );

        // Check overriding to opposite value
        let overridden = DialectOverrides::new($dialect).$method($override_val);
        assert_eq!(
            Dialect::$method(&overridden),
            $override_val,
            "Overridden value for {} should be {}",
            stringify!($method),
            $override_val
        );
    }};
}

#[test]
fn test_override_supports_order_by_all() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_order_by_all, false, true);
}

#[test]
fn test_override_supports_nested_comments() {
    let dialect = MySqlDialect {};
    test_override!(dialect, supports_nested_comments, false, true);
}

#[test]
fn test_override_supports_triple_quoted_string() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_triple_quoted_string, false, true);
}

#[test]
fn test_override_supports_string_literal_backslash_escape() {
    let dialect = PostgreSqlDialect {};
    test_override!(
        dialect,
        supports_string_literal_backslash_escape,
        false,
        true
    );
}

#[test]
fn test_override_supports_filter_during_aggregation() {
    let dialect = MySqlDialect {};
    test_override!(dialect, supports_filter_during_aggregation, false, true);
}

#[test]
fn test_override_supports_group_by_expr() {
    let dialect = SQLiteDialect {};
    test_override!(dialect, supports_group_by_expr, false, true);
}

#[test]
fn test_override_supports_in_empty_list() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_in_empty_list, false, true);
}

#[test]
fn test_override_supports_numeric_prefix() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_numeric_prefix, false, true);
}

#[test]
fn test_override_supports_lambda_functions() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_lambda_functions, false, true);
}

#[test]
fn test_override_supports_dictionary_syntax() {
    let dialect = PostgreSqlDialect {};
    test_override!(dialect, supports_dictionary_syntax, false, true);
}

#[test]
fn test_override_supports_select_wildcard_except() {
    let dialect = PostgreSqlDialect {};
    test_override!(dialect, supports_select_wildcard_except, false, true);
}

#[test]
fn test_override_supports_trailing_commas() {
    let dialect = PostgreSqlDialect {};
    test_override!(dialect, supports_trailing_commas, false, true);
}

#[test]
fn test_override_supports_partiql() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_partiql, false, true);
}

#[test]
fn test_override_multiple_features_chained() {
    let dialect = DialectOverrides::new(GenericDialect {})
        .supports_order_by_all(true)
        .supports_nested_comments(true)
        .supports_triple_quoted_string(true)
        .supports_dictionary_syntax(true)
        .supports_lambda_functions(true);

    assert!(Dialect::supports_order_by_all(&dialect));
    assert!(Dialect::supports_nested_comments(&dialect));
    assert!(Dialect::supports_triple_quoted_string(&dialect));
    assert!(Dialect::supports_dictionary_syntax(&dialect));
    assert!(Dialect::supports_lambda_functions(&dialect));

    // Non-overridden features should still use defaults
    assert!(!Dialect::supports_partiql(&dialect));
}

#[test]
fn test_override_preserves_dialect_identity() {
    let generic = DialectOverrides::new(GenericDialect {}).supports_order_by_all(true);
    let postgres = DialectOverrides::new(PostgreSqlDialect {}).supports_order_by_all(true);
    let mysql = DialectOverrides::new(MySqlDialect {}).supports_order_by_all(true);

    let generic_ref: &dyn Dialect = &generic;
    let postgres_ref: &dyn Dialect = &postgres;
    let mysql_ref: &dyn Dialect = &mysql;

    assert!(generic_ref.is::<GenericDialect>());
    assert!(postgres_ref.is::<PostgreSqlDialect>());
    assert!(mysql_ref.is::<MySqlDialect>());
}

#[test]
fn test_override_inner_dialect_unchanged() {
    let dialect = DialectOverrides::new(GenericDialect {})
        .supports_order_by_all(true)
        .supports_nested_comments(false);

    // The wrapper has overrides
    assert!(Dialect::supports_order_by_all(&dialect));

    // The inner dialect is unchanged
    let inner = dialect.inner();
    assert!(!Dialect::supports_order_by_all(inner));
    assert!(Dialect::supports_nested_comments(inner)); // GenericDialect default is true
}

#[test]
fn test_parse_order_by_all_with_override() {
    let dialect = DialectOverrides::new(GenericDialect {}).supports_order_by_all(true);

    let sql = "SELECT * FROM users ORDER BY ALL";
    let result = Parser::new(&dialect)
        .try_with_sql(sql)
        .unwrap()
        .parse_statements();

    assert!(result.is_ok(), "Should parse ORDER BY ALL successfully");
}

#[test]
fn test_parse_triple_quoted_string_with_override() {
    let dialect = DialectOverrides::new(PostgreSqlDialect {}).supports_triple_quoted_string(true);

    let sql = r#"SELECT """hello world""""#;
    let result = Parser::new(&dialect)
        .try_with_sql(sql)
        .unwrap()
        .parse_statements();

    assert!(
        result.is_ok(),
        "Should parse triple-quoted strings successfully"
    );
}

#[test]
fn test_override_with_all_base_dialects() {
    // Test that overrides work with all built-in dialects
    let dialects: Vec<Box<dyn Dialect>> = vec![
        Box::new(DialectOverrides::new(GenericDialect {}).supports_order_by_all(true)),
        Box::new(DialectOverrides::new(PostgreSqlDialect {}).supports_order_by_all(true)),
        Box::new(DialectOverrides::new(MySqlDialect {}).supports_order_by_all(true)),
        Box::new(DialectOverrides::new(SQLiteDialect {}).supports_order_by_all(true)),
        Box::new(DialectOverrides::new(SnowflakeDialect).supports_order_by_all(true)),
        Box::new(DialectOverrides::new(RedshiftSqlDialect {}).supports_order_by_all(true)),
        Box::new(DialectOverrides::new(MsSqlDialect {}).supports_order_by_all(true)),
        Box::new(DialectOverrides::new(ClickHouseDialect {}).supports_order_by_all(true)),
        Box::new(DialectOverrides::new(BigQueryDialect).supports_order_by_all(true)),
        Box::new(DialectOverrides::new(AnsiDialect {}).supports_order_by_all(true)),
        Box::new(DialectOverrides::new(DuckDbDialect {}).supports_order_by_all(true)),
        Box::new(DialectOverrides::new(HiveDialect {}).supports_order_by_all(true)),
        Box::new(DialectOverrides::new(DatabricksDialect {}).supports_order_by_all(true)),
    ];

    for dialect in dialects {
        assert!(dialect.supports_order_by_all());
    }
}

#[test]
fn test_override_convert_type_before_value() {
    let dialect = GenericDialect {};
    test_override!(dialect, convert_type_before_value, false, true);
}

#[test]
fn test_override_supports_numeric_literal_underscores() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_numeric_literal_underscores, false, true);
}

#[test]
fn test_override_supports_window_function_null_treatment_arg() {
    let dialect = PostgreSqlDialect {};
    test_override!(
        dialect,
        supports_window_function_null_treatment_arg,
        false,
        true
    );
}

#[test]
fn test_override_supports_parenthesized_set_variables() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_parenthesized_set_variables, true, false);
}

#[test]
fn test_override_supports_limit_comma() {
    let dialect = PostgreSqlDialect {};
    test_override!(dialect, supports_limit_comma, false, true);
}

#[test]
fn test_override_supports_struct_literal() {
    let dialect = PostgreSqlDialect {};
    test_override!(dialect, supports_struct_literal, false, true);
}

#[test]
fn test_override_supports_from_first_select() {
    let dialect = PostgreSqlDialect {};
    test_override!(dialect, supports_from_first_select, false, true);
}

#[test]
fn test_override_supports_pipe_operator() {
    let dialect = PostgreSqlDialect {};
    test_override!(dialect, supports_pipe_operator, false, true);
}

#[test]
fn test_override_describe_requires_table_keyword() {
    let dialect = GenericDialect {};
    test_override!(dialect, describe_requires_table_keyword, false, true);
}

#[test]
fn test_override_allow_extract_custom() {
    // Use a dialect that doesn't allow extract custom by default
    let dialect = PostgreSqlDialect {};
    test_override!(dialect, allow_extract_custom, true, false);
}

#[test]
fn test_override_supports_create_index_with_clause() {
    // Use a dialect that doesn't support this by default
    let dialect = MySqlDialect {};
    test_override!(dialect, supports_create_index_with_clause, false, true);
}

#[test]
fn test_override_supports_factorial_operator() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_factorial_operator, false, true);
}

#[test]
fn test_override_supports_eq_alias_assignment() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_eq_alias_assignment, false, true);
}

#[test]
fn test_override_supports_bang_not_operator() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_bang_not_operator, false, true);
}

#[test]
fn test_override_supports_top_before_distinct() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_top_before_distinct, false, true);
}

#[test]
fn test_override_supports_boolean_literals() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_boolean_literals, true, false);
}

#[test]
fn test_override_supports_create_table_select() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_create_table_select, false, true);
}

#[test]
fn test_override_supports_insert_set() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_insert_set, false, true);
}

#[test]
fn test_override_supports_timestamp_versioning() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_timestamp_versioning, false, true);
}

#[test]
fn test_override_supports_geometric_types() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_geometric_types, false, true);
}

#[test]
fn test_override_supports_array_typedef_with_brackets() {
    let dialect = MySqlDialect {};
    test_override!(dialect, supports_array_typedef_with_brackets, false, true);
}

#[test]
fn test_override_supports_interval_options() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_interval_options, true, false);
}

#[test]
fn test_override_delegation_to_base_dialect() {
    // Test that non-overridden methods properly delegate to base dialect
    let postgres = PostgreSqlDialect {};
    let overridden = DialectOverrides::new(postgres).supports_order_by_all(true);

    // PostgreSQL-specific features should still work
    assert!(Dialect::supports_filter_during_aggregation(&overridden));
    assert!(Dialect::supports_unicode_string_literal(&overridden));
    assert!(Dialect::supports_string_escape_constant(&overridden));
}

#[test]
fn test_override_identifier_methods_delegated() {
    let dialect = DialectOverrides::new(GenericDialect {}).supports_order_by_all(true);

    // These should be delegated to GenericDialect
    assert!(dialect.is_identifier_start('a'));
    assert!(dialect.is_identifier_start('_'));
    assert!(dialect.is_identifier_part('1'));
    assert!(dialect.is_delimited_identifier_start('"'));
}

#[test]
fn test_override_parse_methods_delegated() {
    let dialect = DialectOverrides::new(GenericDialect {}).supports_order_by_all(true);

    let sql = "SELECT 1 + 2";
    let result = Parser::new(&dialect)
        .try_with_sql(sql)
        .unwrap()
        .parse_statements();

    assert!(result.is_ok());

    if let Ok(stmts) = result {
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            Statement::Query(query) => {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    assert_eq!(select.projection.len(), 1);
                }
            }
            _ => panic!("Expected SELECT statement"),
        }
    }
}

#[test]
fn test_override_supports_named_fn_args_operators() {
    let dialect = GenericDialect {};

    // Test all four named function argument operators
    let with_eq =
        DialectOverrides::new(GenericDialect {}).supports_named_fn_args_with_eq_operator(true);
    assert!(Dialect::supports_named_fn_args_with_eq_operator(&with_eq));

    let with_colon =
        DialectOverrides::new(GenericDialect {}).supports_named_fn_args_with_colon_operator(true);
    assert!(Dialect::supports_named_fn_args_with_colon_operator(
        &with_colon
    ));

    let with_assignment = DialectOverrides::new(GenericDialect {})
        .supports_named_fn_args_with_assignment_operator(true);
    assert!(Dialect::supports_named_fn_args_with_assignment_operator(
        &with_assignment
    ));

    let with_rarrow =
        DialectOverrides::new(GenericDialect {}).supports_named_fn_args_with_rarrow_operator(true);
    assert!(Dialect::supports_named_fn_args_with_rarrow_operator(
        &with_rarrow
    ));
}

#[test]
fn test_override_supports_join_variations() {
    let dialect = GenericDialect {};

    let with_outer_join =
        DialectOverrides::new(GenericDialect {}).supports_outer_join_operator(true);
    assert!(Dialect::supports_outer_join_operator(&with_outer_join));

    let with_cross_join =
        DialectOverrides::new(GenericDialect {}).supports_cross_join_constraint(true);
    assert!(Dialect::supports_cross_join_constraint(&with_cross_join));

    let with_left_assoc = DialectOverrides::new(GenericDialect {})
        .supports_left_associative_joins_without_parens(false);
    assert!(!Dialect::supports_left_associative_joins_without_parens(
        &with_left_assoc
    ));
}

#[test]
fn test_override_supports_transaction_modifiers() {
    let dialect = GenericDialect {};

    let with_start =
        DialectOverrides::new(GenericDialect {}).supports_start_transaction_modifier(true);
    assert!(Dialect::supports_start_transaction_modifier(&with_start));

    let with_end = DialectOverrides::new(GenericDialect {}).supports_end_transaction_modifier(true);
    assert!(Dialect::supports_end_transaction_modifier(&with_end));
}

#[test]
fn test_override_supports_wildcard_variations() {
    let dialect = PostgreSqlDialect {};

    let with_except =
        DialectOverrides::new(PostgreSqlDialect {}).supports_select_wildcard_except(true);
    assert!(Dialect::supports_select_wildcard_except(&with_except));

    let with_exclude =
        DialectOverrides::new(PostgreSqlDialect {}).supports_select_wildcard_exclude(true);
    assert!(Dialect::supports_select_wildcard_exclude(&with_exclude));

    let with_select_exclude =
        DialectOverrides::new(PostgreSqlDialect {}).supports_select_exclude(true);
    assert!(Dialect::supports_select_exclude(&with_select_exclude));
}

#[test]
fn test_override_supports_trailing_comma_variations() {
    let dialect = PostgreSqlDialect {};

    let with_general =
        DialectOverrides::new(PostgreSqlDialect {}).supports_trailing_commas(true);
    assert!(Dialect::supports_trailing_commas(&with_general));

    let with_projection =
        DialectOverrides::new(PostgreSqlDialect {}).supports_projection_trailing_commas(true);
    assert!(Dialect::supports_projection_trailing_commas(&with_projection));

    let with_from = DialectOverrides::new(PostgreSqlDialect {}).supports_from_trailing_commas(true);
    assert!(Dialect::supports_from_trailing_commas(&with_from));

    let with_column_def =
        DialectOverrides::new(PostgreSqlDialect {}).supports_column_definition_trailing_commas(true);
    assert!(Dialect::supports_column_definition_trailing_commas(
        &with_column_def
    ));
}

#[test]
fn test_override_supports_special_syntax() {
    let dialect = GenericDialect {};

    let with_connect_by = DialectOverrides::new(GenericDialect {}).supports_connect_by(true);
    assert!(Dialect::supports_connect_by(&with_connect_by));

    let with_match_recognize =
        DialectOverrides::new(GenericDialect {}).supports_match_recognize(true);
    assert!(Dialect::supports_match_recognize(&with_match_recognize));

    let with_execute_immediate =
        DialectOverrides::new(GenericDialect {}).supports_execute_immediate(true);
    assert!(Dialect::supports_execute_immediate(&with_execute_immediate));
}

#[test]
fn test_override_supports_string_variations() {
    let dialect = PostgreSqlDialect {};

    let with_unicode =
        DialectOverrides::new(PostgreSqlDialect {}).supports_unicode_string_literal(true);
    assert!(Dialect::supports_unicode_string_literal(&with_unicode));

    let with_concat =
        DialectOverrides::new(PostgreSqlDialect {}).supports_string_literal_concatenation(true);
    assert!(Dialect::supports_string_literal_concatenation(&with_concat));

    let with_escape =
        DialectOverrides::new(PostgreSqlDialect {}).supports_string_escape_constant(true);
    assert!(Dialect::supports_string_escape_constant(&with_escape));
}

#[test]
fn test_override_supports_set_variations() {
    let dialect = GenericDialect {};

    let with_parenthesized =
        DialectOverrides::new(GenericDialect {}).supports_parenthesized_set_variables(true);
    assert!(Dialect::supports_parenthesized_set_variables(
        &with_parenthesized
    ));

    let with_comma_separated =
        DialectOverrides::new(GenericDialect {}).supports_comma_separated_set_assignments(true);
    assert!(Dialect::supports_comma_separated_set_assignments(
        &with_comma_separated
    ));

    let with_set_names = DialectOverrides::new(GenericDialect {}).supports_set_names(true);
    assert!(Dialect::supports_set_names(&with_set_names));

    let without_operator =
        DialectOverrides::new(GenericDialect {}).supports_set_stmt_without_operator(true);
    assert!(Dialect::supports_set_stmt_without_operator(&without_operator));
}

#[test]
fn test_override_supports_mysql_specific() {
    let dialect = GenericDialect {};

    let with_user_host = DialectOverrides::new(GenericDialect {}).supports_user_host_grantee(true);
    assert!(Dialect::supports_user_host_grantee(&with_user_host));

    let with_match_against =
        DialectOverrides::new(GenericDialect {}).supports_match_against(true);
    assert!(Dialect::supports_match_against(&with_match_against));

    let with_load_data = DialectOverrides::new(GenericDialect {}).supports_load_data(true);
    assert!(Dialect::supports_load_data(&with_load_data));
}

#[test]
fn test_override_supports_postgres_specific() {
    let dialect = GenericDialect {};

    let with_listen = DialectOverrides::new(GenericDialect {}).supports_listen_notify(true);
    assert!(Dialect::supports_listen_notify(&with_listen));

    let with_comment_on = DialectOverrides::new(GenericDialect {}).supports_comment_on(true);
    assert!(Dialect::supports_comment_on(&with_comment_on));

    let with_alter_using =
        DialectOverrides::new(GenericDialect {}).supports_alter_column_type_using(true);
    assert!(Dialect::supports_alter_column_type_using(&with_alter_using));
}

#[test]
fn test_override_supports_window_variations() {
    let dialect = GenericDialect {};

    let with_named_window = DialectOverrides::new(GenericDialect {})
        .supports_window_clause_named_window_reference(true);
    assert!(Dialect::supports_window_clause_named_window_reference(
        &with_named_window
    ));

    let with_null_treatment = DialectOverrides::new(GenericDialect {})
        .supports_window_function_null_treatment_arg(true);
    assert!(Dialect::supports_window_function_null_treatment_arg(
        &with_null_treatment
    ));
}

#[test]
fn test_override_supports_aggregation_variations() {
    let dialect = GenericDialect {};

    let with_filter =
        DialectOverrides::new(GenericDialect {}).supports_filter_during_aggregation(true);
    assert!(Dialect::supports_filter_during_aggregation(&with_filter));

    let with_within =
        DialectOverrides::new(GenericDialect {}).supports_within_after_array_aggregation(true);
    assert!(Dialect::supports_within_after_array_aggregation(&with_within));
}

#[test]
fn test_override_supports_group_by_variations() {
    let dialect = SQLiteDialect {};

    let with_expr = DialectOverrides::new(SQLiteDialect {}).supports_group_by_expr(true);
    assert!(Dialect::supports_group_by_expr(&with_expr));

    let with_modifier =
        DialectOverrides::new(SQLiteDialect {}).supports_group_by_with_modifier(true);
    assert!(Dialect::supports_group_by_with_modifier(&with_modifier));
}

#[test]
fn test_override_supports_insert_variations() {
    let dialect = GenericDialect {};

    let with_set = DialectOverrides::new(GenericDialect {}).supports_insert_set(true);
    assert!(Dialect::supports_insert_set(&with_set));

    let with_table_fn =
        DialectOverrides::new(GenericDialect {}).supports_insert_table_function(true);
    assert!(Dialect::supports_insert_table_function(&with_table_fn));

    let with_format = DialectOverrides::new(GenericDialect {}).supports_insert_format(true);
    assert!(Dialect::supports_insert_format(&with_format));
}

#[test]
fn test_override_supports_data_type_variations() {
    let dialect = GenericDialect {};

    let with_signed = DialectOverrides::new(GenericDialect {}).supports_data_type_signed_suffix(true);
    assert!(Dialect::supports_data_type_signed_suffix(&with_signed));

    let with_interval_options =
        DialectOverrides::new(GenericDialect {}).supports_interval_options(false);
    assert!(!Dialect::supports_interval_options(&with_interval_options));
}

#[test]
fn test_override_supports_create_table_variations() {
    let dialect = GenericDialect {};

    let with_multi_schema = DialectOverrides::new(GenericDialect {})
        .supports_create_table_multi_schema_info_sources(true);
    assert!(Dialect::supports_create_table_multi_schema_info_sources(
        &with_multi_schema
    ));

    let with_like_parens =
        DialectOverrides::new(GenericDialect {}).supports_create_table_like_parenthesized(true);
    assert!(Dialect::supports_create_table_like_parenthesized(
        &with_like_parens
    ));
}

#[test]
fn test_override_supports_misc_features() {
    let dialect = GenericDialect {};

    let with_dollar_placeholder =
        DialectOverrides::new(GenericDialect {}).supports_dollar_placeholder(true);
    assert!(Dialect::supports_dollar_placeholder(&with_dollar_placeholder));

    let with_object_name_double_dot =
        DialectOverrides::new(GenericDialect {}).supports_object_name_double_dot_notation(true);
    assert!(Dialect::supports_object_name_double_dot_notation(
        &with_object_name_double_dot
    ));

    let with_empty_projections =
        DialectOverrides::new(GenericDialect {}).supports_empty_projections(true);
    assert!(Dialect::supports_empty_projections(&with_empty_projections));

    let with_expr_star = DialectOverrides::new(GenericDialect {}).supports_select_expr_star(true);
    assert!(Dialect::supports_select_expr_star(&with_expr_star));
}

#[test]
fn test_override_chaining_order_independence() {
    // Test that chaining order doesn't matter
    let dialect1 = DialectOverrides::new(GenericDialect {})
        .supports_order_by_all(true)
        .supports_nested_comments(false)
        .supports_triple_quoted_string(true);

    let dialect2 = DialectOverrides::new(GenericDialect {})
        .supports_triple_quoted_string(true)
        .supports_order_by_all(true)
        .supports_nested_comments(false);

    assert_eq!(
        Dialect::supports_order_by_all(&dialect1),
        Dialect::supports_order_by_all(&dialect2)
    );
    assert_eq!(
        Dialect::supports_nested_comments(&dialect1),
        Dialect::supports_nested_comments(&dialect2)
    );
    assert_eq!(
        Dialect::supports_triple_quoted_string(&dialect1),
        Dialect::supports_triple_quoted_string(&dialect2)
    );
}

#[test]
fn test_override_complex_sql_parsing() {
    // Test a complex SQL statement with multiple overrides
    let dialect = DialectOverrides::new(PostgreSqlDialect {})
        .supports_order_by_all(true)
        .supports_filter_during_aggregation(true)
        .supports_triple_quoted_string(true);

    let sql = r#"
        SELECT
            customer_id,
            COUNT(*) FILTER (WHERE status = 'active') as active_count
        FROM customers
        GROUP BY customer_id
        ORDER BY ALL
    "#;

    let result = Parser::new(&dialect)
        .try_with_sql(sql)
        .unwrap()
        .parse_statements();

    assert!(result.is_ok(), "Complex SQL should parse successfully");
}

#[test]
fn test_override_map_literal_syntax() {
    let dialect = GenericDialect {};
    test_override!(dialect, support_map_literal_syntax, true, false);
}

#[test]
fn test_override_supports_explain_with_utility_options() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_explain_with_utility_options, true, false);
}

#[test]
fn test_override_supports_asc_desc_in_column_definition() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_asc_desc_in_column_definition, true, false);
}

#[test]
fn test_override_supports_try_convert() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_try_convert, true, false);
}

#[test]
fn test_override_supports_table_sample_before_alias() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_table_sample_before_alias, false, true);
}

#[test]
fn test_override_supports_space_separated_column_options() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_space_separated_column_options, false, true);
}

#[test]
fn test_override_supports_comma_separated_drop_column_list() {
    let dialect = GenericDialect {};
    test_override!(
        dialect,
        supports_comma_separated_drop_column_list,
        false,
        true
    );
}

#[test]
fn test_override_supports_notnull_operator() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_notnull_operator, false, true);
}

#[test]
fn test_override_supports_semantic_view_table_factor() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_semantic_view_table_factor, false, true);
}

#[test]
fn test_override_ignores_wildcard_escapes() {
    let dialect = GenericDialect {};
    test_override!(dialect, ignores_wildcard_escapes, false, true);
}

#[test]
fn test_override_require_interval_qualifier() {
    let dialect = GenericDialect {};
    test_override!(dialect, require_interval_qualifier, false, true);
}

#[test]
fn test_override_requires_single_line_comment_whitespace() {
    let dialect = GenericDialect {};
    test_override!(dialect, requires_single_line_comment_whitespace, false, true);
}

#[test]
fn test_override_supports_show_like_before_in() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_show_like_before_in, false, true);
}

#[test]
fn test_override_supports_table_hints() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_table_hints, false, true);
}

#[test]
fn test_override_allow_extract_single_quotes() {
    // Use a dialect that doesn't allow extract single quotes by default
    let dialect = MySqlDialect {};
    test_override!(dialect, allow_extract_single_quotes, false, true);
}

#[test]
fn test_override_supports_load_extension() {
    // Use a dialect that doesn't support load extension by default
    let dialect = MySqlDialect {};
    test_override!(dialect, supports_load_extension, false, true);
}

#[test]
fn test_override_supports_named_fn_args_with_expr_name() {
    let dialect = GenericDialect {};
    test_override!(dialect, supports_named_fn_args_with_expr_name, false, true);
}

#[test]
fn test_override_dialect_of_macro() {
    // Test that dialect_of! macro works with DialectOverrides
    // This simulates what parser code would do internally

    struct ParserLike<'a> {
        dialect: &'a dyn Dialect,
    }

    macro_rules! dialect_of {
        ( $parsed_dialect: ident is $($dialect_type: ty)|+ ) => {
            ($($parsed_dialect.dialect.is::<$dialect_type>())||+)
        };
    }

    // Test with GenericDialect wrapped in DialectOverrides
    let generic_override = DialectOverrides::new(GenericDialect {}).supports_order_by_all(true);
    let parser_with_generic = ParserLike {
        dialect: &generic_override,
    };

    assert!(dialect_of!(parser_with_generic is GenericDialect));
    assert!(!dialect_of!(parser_with_generic is PostgreSqlDialect));
    assert!(dialect_of!(parser_with_generic is GenericDialect | PostgreSqlDialect));

    // Test with PostgreSqlDialect wrapped in DialectOverrides
    let postgres_override = DialectOverrides::new(PostgreSqlDialect {}).supports_order_by_all(true);
    let parser_with_postgres = ParserLike {
        dialect: &postgres_override,
    };

    assert!(dialect_of!(parser_with_postgres is PostgreSqlDialect));
    assert!(!dialect_of!(parser_with_postgres is GenericDialect));
    assert!(dialect_of!(parser_with_postgres is GenericDialect | PostgreSqlDialect));

    // Test with MySqlDialect wrapped in DialectOverrides
    let mysql_override = DialectOverrides::new(MySqlDialect {}).supports_order_by_all(true);
    let parser_with_mysql = ParserLike {
        dialect: &mysql_override,
    };

    assert!(dialect_of!(parser_with_mysql is MySqlDialect));
    assert!(!dialect_of!(parser_with_mysql is PostgreSqlDialect));
    assert!(!dialect_of!(parser_with_mysql is GenericDialect));
    assert!(dialect_of!(parser_with_mysql is MySqlDialect | PostgreSqlDialect | GenericDialect));
}

#[test]
fn test_override_dialect_of_with_multiple_dialects() {
    // Test dialect_of! with various combinations

    struct ParserLike<'a> {
        dialect: &'a dyn Dialect,
    }

    macro_rules! dialect_of {
        ( $parsed_dialect: ident is $($dialect_type: ty)|+ ) => {
            ($($parsed_dialect.dialect.is::<$dialect_type>())||+)
        };
    }

    let overridden_dialects: Vec<(&str, Box<dyn Dialect>)> = vec![
        (
            "GenericDialect",
            Box::new(DialectOverrides::new(GenericDialect {}).supports_order_by_all(true)),
        ),
        (
            "PostgreSqlDialect",
            Box::new(DialectOverrides::new(PostgreSqlDialect {}).supports_order_by_all(true)),
        ),
        (
            "MySqlDialect",
            Box::new(DialectOverrides::new(MySqlDialect {}).supports_order_by_all(true)),
        ),
        (
            "SQLiteDialect",
            Box::new(DialectOverrides::new(SQLiteDialect {}).supports_order_by_all(true)),
        ),
        (
            "SnowflakeDialect",
            Box::new(DialectOverrides::new(SnowflakeDialect).supports_order_by_all(true)),
        ),
        (
            "BigQueryDialect",
            Box::new(DialectOverrides::new(BigQueryDialect).supports_order_by_all(true)),
        ),
    ];

    for (name, dialect) in &overridden_dialects {
        let parser = ParserLike {
            dialect: dialect.as_ref(),
        };

        // Each should match at least one dialect type
        let matches_something = match name.as_ref() {
            "GenericDialect" => dialect_of!(parser is GenericDialect),
            "PostgreSqlDialect" => dialect_of!(parser is PostgreSqlDialect),
            "MySqlDialect" => dialect_of!(parser is MySqlDialect),
            "SQLiteDialect" => dialect_of!(parser is SQLiteDialect),
            "SnowflakeDialect" => dialect_of!(parser is SnowflakeDialect),
            "BigQueryDialect" => dialect_of!(parser is BigQueryDialect),
            _ => false,
        };

        assert!(matches_something, "{} should match its own type", name);
    }
}

#[test]
fn test_override_is_identifier_start() {
    let dialect = GenericDialect {};

    // Default GenericDialect allows letters and underscores
    assert!(Dialect::is_identifier_start(&dialect, 'a'));
    assert!(Dialect::is_identifier_start(&dialect, '_'));
    assert!(!Dialect::is_identifier_start(&dialect, '1'));

    // Override to also allow dollar signs
    let overridden = DialectOverrides::new(GenericDialect {})
        .with_is_identifier_start(|ch| ch.is_alphabetic() || ch == '_' || ch == '$');

    assert!(Dialect::is_identifier_start(&overridden, 'a'));
    assert!(Dialect::is_identifier_start(&overridden, '_'));
    assert!(Dialect::is_identifier_start(&overridden, '$')); // Now allowed
    assert!(!Dialect::is_identifier_start(&overridden, '1'));
}

#[test]
fn test_override_is_identifier_part() {
    let dialect = GenericDialect {};

    // Override to disallow numbers in identifiers (unusual but possible)
    let overridden = DialectOverrides::new(GenericDialect {})
        .with_is_identifier_part(|ch| ch.is_alphabetic() || ch == '_');

    assert!(Dialect::is_identifier_part(&overridden, 'a'));
    assert!(Dialect::is_identifier_part(&overridden, '_'));
    assert!(!Dialect::is_identifier_part(&overridden, '1')); // No longer allowed
}

#[test]
fn test_override_is_delimited_identifier_start() {
    let dialect = GenericDialect {};

    // Default allows " and `
    assert!(Dialect::is_delimited_identifier_start(&dialect, '"'));
    assert!(Dialect::is_delimited_identifier_start(&dialect, '`'));
    assert!(!Dialect::is_delimited_identifier_start(&dialect, '['));

    // Override to also allow square brackets
    let overridden = DialectOverrides::new(GenericDialect {})
        .with_is_delimited_identifier_start(|ch| ch == '"' || ch == '`' || ch == '[');

    assert!(Dialect::is_delimited_identifier_start(&overridden, '"'));
    assert!(Dialect::is_delimited_identifier_start(&overridden, '`'));
    assert!(Dialect::is_delimited_identifier_start(&overridden, '[')); // Now allowed
}

#[test]
fn test_override_is_custom_operator_part() {
    let dialect = GenericDialect {};

    // Default doesn't allow custom operators
    assert!(!Dialect::is_custom_operator_part(&dialect, '@'));

    // Override to allow @ as custom operator
    let overridden = DialectOverrides::new(GenericDialect {})
        .with_is_custom_operator_part(|ch| ch == '@' || ch == '~');

    assert!(Dialect::is_custom_operator_part(&overridden, '@'));
    assert!(Dialect::is_custom_operator_part(&overridden, '~'));
    assert!(!Dialect::is_custom_operator_part(&overridden, '#'));
}

#[test]
fn test_override_identifier_quote_style() {
    let dialect = GenericDialect {};

    // Default returns None
    assert_eq!(Dialect::identifier_quote_style(&dialect, "foo"), None);

    // Override to always use backticks
    let overridden = DialectOverrides::new(GenericDialect {})
        .with_identifier_quote_style(|_id| Some('`'));

    assert_eq!(Dialect::identifier_quote_style(&overridden, "foo"), Some('`'));
    assert_eq!(Dialect::identifier_quote_style(&overridden, "bar"), Some('`'));
}

#[test]
fn test_override_identifier_quote_style_conditional() {
    // Override to use different quotes based on identifier content
    let overridden = DialectOverrides::new(GenericDialect {})
        .with_identifier_quote_style(|id| {
            if id.contains(' ') {
                Some('"') // Use double quotes for identifiers with spaces
            } else if id.chars().any(|ch| ch.is_uppercase()) {
                Some('`') // Use backticks for identifiers with uppercase
            } else {
                None // No quotes needed
            }
        });

    assert_eq!(Dialect::identifier_quote_style(&overridden, "simple"), None);
    assert_eq!(Dialect::identifier_quote_style(&overridden, "My Table"), Some('"'));
    assert_eq!(Dialect::identifier_quote_style(&overridden, "MyColumn"), Some('`'));
}

#[test]
fn test_override_prec_unknown() {
    let dialect = GenericDialect {};

    // Default is 0
    assert_eq!(Dialect::prec_unknown(&dialect), 0);

    // Override to use a different value
    let overridden = DialectOverrides::new(GenericDialect {})
        .with_prec_unknown(10);

    assert_eq!(Dialect::prec_unknown(&overridden), 10);
}

#[test]
fn test_override_reserved_keywords_for_select_item_operator() {
    use sqlparser::keywords::Keyword;

    let dialect = GenericDialect {};

    // Default returns empty slice
    assert_eq!(dialect.get_reserved_keywords_for_select_item_operator().len(), 0);

    // Override with custom keywords
    static CUSTOM_KEYWORDS: &[Keyword] = &[Keyword::CONNECT_BY_ROOT];
    let overridden = DialectOverrides::new(GenericDialect {})
        .with_reserved_keywords_for_select_item_operator(CUSTOM_KEYWORDS);

    assert_eq!(overridden.get_reserved_keywords_for_select_item_operator().len(), 1);
    assert_eq!(overridden.get_reserved_keywords_for_select_item_operator()[0], Keyword::CONNECT_BY_ROOT);
}

#[test]
fn test_override_reserved_grantees_types() {
    use sqlparser::ast::GranteesType;

    let dialect = GenericDialect {};

    // Default returns empty slice
    assert_eq!(dialect.get_reserved_grantees_types().len(), 0);

    // Override with custom types
    static CUSTOM_TYPES: &[GranteesType] = &[GranteesType::Public];
    let overridden = DialectOverrides::new(GenericDialect {})
        .with_reserved_grantees_types(CUSTOM_TYPES);

    assert_eq!(overridden.get_reserved_grantees_types().len(), 1);
    assert_eq!(overridden.get_reserved_grantees_types()[0], GranteesType::Public);
}

#[test]
fn test_override_chaining_with_functions() {
    // Test that function overrides can be chained with boolean overrides
    let dialect = DialectOverrides::new(GenericDialect {})
        .with_is_identifier_start(|ch| ch.is_alphabetic() || ch == '_' || ch == '$')
        .with_identifier_quote_style(|_| Some('`'))
        .supports_order_by_all(true)
        .supports_nested_comments(true);

    // Check function overrides work
    assert!(Dialect::is_identifier_start(&dialect, '$'));
    assert_eq!(Dialect::identifier_quote_style(&dialect, "foo"), Some('`'));

    // Check boolean overrides still work
    assert!(Dialect::supports_order_by_all(&dialect));
    assert!(Dialect::supports_nested_comments(&dialect));
}

#[test]
fn test_override_parsing_with_custom_identifier_rules() {
    // Create a dialect that allows $ in identifiers
    let dialect = DialectOverrides::new(GenericDialect {})
        .with_is_identifier_start(|ch| ch.is_alphabetic() || ch == '_' || ch == '$')
        .with_is_identifier_part(|ch| ch.is_alphanumeric() || ch == '_' || ch == '$');

    // This should parse successfully with our custom rules
    let sql = "SELECT $column FROM $table";
    let result = Parser::new(&dialect)
        .try_with_sql(sql)
        .unwrap()
        .parse_statements();

    assert!(result.is_ok(), "Should parse identifiers with $ successfully");
}

#[test]
fn test_override_all_function_types() {
    // Stress test: override all function-type methods at once
    static KEYWORDS: &[sqlparser::keywords::Keyword] = &[];
    static GRANTEES: &[sqlparser::ast::GranteesType] = &[];

    let dialect = DialectOverrides::new(GenericDialect {})
        .with_is_identifier_start(|ch| ch.is_alphabetic())
        .with_is_identifier_part(|ch| ch.is_alphanumeric())
        .with_is_delimited_identifier_start(|ch| ch == '"')
        .with_is_custom_operator_part(|ch| ch == '@')
        .with_identifier_quote_style(|_| Some('`'))
        .with_reserved_keywords_for_select_item_operator(KEYWORDS)
        .with_reserved_grantees_types(GRANTEES)
        .with_prec_unknown(5);

    // Verify all overrides are active
    assert!(Dialect::is_identifier_start(&dialect, 'a'));
    assert!(!Dialect::is_identifier_start(&dialect, '_')); // Overridden to disallow
    assert!(Dialect::is_identifier_part(&dialect, '1'));
    assert!(!Dialect::is_identifier_part(&dialect, '_')); // Overridden to disallow
    assert!(Dialect::is_delimited_identifier_start(&dialect, '"'));
    assert!(!Dialect::is_delimited_identifier_start(&dialect, '`')); // Overridden to disallow
    assert!(Dialect::is_custom_operator_part(&dialect, '@'));
    assert_eq!(Dialect::identifier_quote_style(&dialect, "test"), Some('`'));
    assert_eq!(dialect.get_reserved_keywords_for_select_item_operator().len(), 0);
    assert_eq!(dialect.get_reserved_grantees_types().len(), 0);
    assert_eq!(Dialect::prec_unknown(&dialect), 5);
}

#[test]
fn test_override_all_features_at_once() {
    // This is a stress test to ensure all overrides can be set simultaneously
    let dialect = DialectOverrides::new(GenericDialect {})
        .supports_string_literal_backslash_escape(true)
        .ignores_wildcard_escapes(true)
        .supports_unicode_string_literal(true)
        .supports_filter_during_aggregation(true)
        .supports_window_clause_named_window_reference(true)
        .supports_within_after_array_aggregation(true)
        .supports_group_by_expr(true)
        .supports_group_by_with_modifier(true)
        .supports_left_associative_joins_without_parens(false)
        .supports_outer_join_operator(true)
        .supports_cross_join_constraint(true)
        .supports_connect_by(true)
        .supports_execute_immediate(true)
        .supports_match_recognize(true)
        .supports_in_empty_list(true)
        .supports_start_transaction_modifier(true)
        .supports_end_transaction_modifier(true)
        .supports_named_fn_args_with_eq_operator(true)
        .supports_named_fn_args_with_colon_operator(true)
        .supports_named_fn_args_with_assignment_operator(true)
        .supports_named_fn_args_with_rarrow_operator(true)
        .supports_named_fn_args_with_expr_name(true)
        .supports_numeric_prefix(true)
        .supports_numeric_literal_underscores(true)
        .supports_window_function_null_treatment_arg(true)
        .supports_dictionary_syntax(true)
        .support_map_literal_syntax(true)
        .supports_lambda_functions(true)
        .supports_parenthesized_set_variables(true)
        .supports_comma_separated_set_assignments(true)
        .supports_select_wildcard_except(true)
        .convert_type_before_value(true)
        .supports_triple_quoted_string(true)
        .supports_trailing_commas(true)
        .supports_limit_comma(true)
        .supports_string_literal_concatenation(true)
        .supports_projection_trailing_commas(true)
        .supports_from_trailing_commas(true)
        .supports_column_definition_trailing_commas(true)
        .supports_object_name_double_dot_notation(true)
        .supports_struct_literal(true)
        .supports_empty_projections(true)
        .supports_select_expr_star(true)
        .supports_from_first_select(true)
        .supports_pipe_operator(true)
        .supports_user_host_grantee(true)
        .supports_match_against(true)
        .supports_select_wildcard_exclude(true)
        .supports_select_exclude(true)
        .supports_create_table_multi_schema_info_sources(true)
        .describe_requires_table_keyword(true)
        .allow_extract_custom(true)
        .allow_extract_single_quotes(true)
        .supports_dollar_placeholder(true)
        .supports_create_index_with_clause(true)
        .require_interval_qualifier(true)
        .supports_explain_with_utility_options(true)
        .supports_asc_desc_in_column_definition(true)
        .supports_factorial_operator(true)
        .supports_nested_comments(true)
        .supports_eq_alias_assignment(true)
        .supports_try_convert(true)
        .supports_bang_not_operator(true)
        .supports_listen_notify(true)
        .supports_load_data(true)
        .supports_load_extension(true)
        .supports_top_before_distinct(true)
        .supports_boolean_literals(false)
        .supports_show_like_before_in(true)
        .supports_comment_on(true)
        .supports_create_table_select(true)
        .supports_partiql(true)
        .supports_table_sample_before_alias(true)
        .supports_insert_set(true)
        .supports_insert_table_function(true)
        .supports_insert_format(true)
        .supports_set_stmt_without_operator(true)
        .supports_timestamp_versioning(true)
        .supports_string_escape_constant(true)
        .supports_table_hints(true)
        .requires_single_line_comment_whitespace(true)
        .supports_array_typedef_with_brackets(true)
        .supports_geometric_types(true)
        .supports_order_by_all(true)
        .supports_set_names(true)
        .supports_space_separated_column_options(true)
        .supports_alter_column_type_using(true)
        .supports_comma_separated_drop_column_list(true)
        .supports_notnull_operator(true)
        .supports_data_type_signed_suffix(true)
        .supports_interval_options(true)
        .supports_create_table_like_parenthesized(true)
        .supports_semantic_view_table_factor(true);

    // Verify a few key overrides are set
    assert!(Dialect::supports_order_by_all(&dialect));
    assert!(Dialect::supports_partiql(&dialect));
    assert!(!Dialect::supports_boolean_literals(&dialect));
    assert!(Dialect::supports_nested_comments(&dialect));
}
