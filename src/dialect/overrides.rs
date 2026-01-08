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

//! Dialect overrides for customizing dialect behavior
//!
//! This module provides a builder pattern for overriding specific dialect
//! behaviors without implementing the entire [`Dialect`] trait.
//!
//! # Example
//!
//! ```
//! use sqlparser::dialect::{GenericDialect, DialectOverrides};
//! use sqlparser::parser::Parser;
//!
//! // Create a GenericDialect with ORDER BY ALL support
//! let dialect = DialectOverrides::new(GenericDialect)
//!     .supports_order_by_all(true);
//!
//! let sql = "SELECT * FROM my_table ORDER BY ALL";
//! let ast = Parser::new(&dialect)
//!     .try_with_sql(sql)
//!     .unwrap()
//!     .parse_statements()
//!     .unwrap();
//! ```
//!
//! # Creating Custom Dialects with Unique Identity
//!
//! You can use the [`derive_dialect!`] macro to easily create a custom dialect
//! that wraps an existing dialect but has its own unique `TypeId`:
//!
//! ```
//! use sqlparser::dialect::{GenericDialect, DialectOverrides, derive_dialect};
//!
//! // Create a custom dialect that wraps GenericDialect
//! derive_dialect!(MyCustomDialect, GenericDialect);
//!
//! // Use it with DialectOverrides
//! let dialect = DialectOverrides::new(MyCustomDialect::new())
//!     .supports_order_by_all(true);
//! ```

use core::any::TypeId;
use core::fmt::Debug;
use core::iter::Peekable;
use core::str::Chars;

#[cfg(not(feature = "std"))]
use alloc::boxed::Box;

use crate::ast::{ColumnOption, Expr, GranteesType, Ident, ObjectNamePart, Statement};
use crate::keywords::Keyword;
use crate::parser::{Parser, ParserError};

use super::Dialect;

/// Macro to create a custom dialect that wraps an existing dialect.
///
/// This macro generates all the boilerplate needed to create a new dialect type
/// that delegates to an existing dialect but has its own unique `TypeId`. This is
/// useful when you want `dialect_of!` checks to recognize your custom dialect.
///
/// # Syntax
///
/// ```text
/// derive_dialect!(CustomDialectName, BaseDialect);
/// derive_dialect!(CustomDialectName, BaseDialect, optional_methods: { ... });
/// ```
///
/// **Note**: The base dialect type must implement `Default`. Dialects that implement
/// `Default` include: `GenericDialect`, `BigQueryDialect`, `DatabricksDialect`,
/// `DuckDbDialect`, and `SnowflakeDialect`.
///
/// # Examples
///
/// Basic usage:
///
/// ```
/// // Can import from crate root or from dialect module
/// use sqlparser::derive_dialect;
/// // Or: use sqlparser::dialect::derive_dialect;
/// use sqlparser::dialect::{GenericDialect, DialectOverrides, Dialect};
///
/// // Create a custom dialect
/// derive_dialect!(MyDialect, GenericDialect);
///
/// let dialect = MyDialect::new();
/// assert!(dialect.is_identifier_start('a'));
///
/// // Use with DialectOverrides
/// let enhanced = DialectOverrides::new(MyDialect::new())
///     .supports_order_by_all(true);
/// assert!(Dialect::supports_order_by_all(&enhanced));
/// ```
///
/// With optional method overrides:
///
/// ```
/// use sqlparser::derive_dialect;
/// use sqlparser::dialect::GenericDialect;
///
/// derive_dialect!(MyDialect, GenericDialect, optional_methods: {
///     fn supports_filter_during_aggregation(&self) -> bool {
///         true  // Override specific behavior
///     }
/// });
///
/// let dialect = MyDialect::new();
/// ```
#[macro_export]
macro_rules! derive_dialect {
    // Basic form: just derive with all methods delegated
    // Users pass a type that implements Default
    ($name:ident, $base_ty:ty) => {
        #[derive(Debug)]
        pub struct $name {
            _inner: $base_ty,
        }

        impl Default for $name {
            fn default() -> Self {
                Self {
                    _inner: <$base_ty>::default(),
                }
            }
        }

        impl $name {
            pub fn new() -> Self {
                Self::default()
            }
        }

        impl $crate::dialect::Dialect for $name {
            fn is_identifier_start(&self, ch: char) -> bool {
                self._inner.is_identifier_start(ch)
            }

            fn is_identifier_part(&self, ch: char) -> bool {
                self._inner.is_identifier_part(ch)
            }
        }
    };

    // Extended form: with optional method overrides
    ($name:ident, $base_ty:ty, optional_methods: { $($method:tt)* }) => {
        #[derive(Debug)]
        pub struct $name {
            _inner: $base_ty,
        }

        impl Default for $name {
            fn default() -> Self {
                Self {
                    _inner: <$base_ty>::default(),
                }
            }
        }

        impl $name {
            pub fn new() -> Self {
                Self::default()
            }
        }

        impl $crate::dialect::Dialect for $name {
            fn is_identifier_start(&self, ch: char) -> bool {
                self._inner.is_identifier_start(ch)
            }

            fn is_identifier_part(&self, ch: char) -> bool {
                self._inner.is_identifier_part(ch)
            }

            // User-provided optional methods
            $($method)*
        }
    };
}

/// A wrapper around a dialect that allows overriding specific behaviors.
///
/// Each field represents an optional override for a corresponding method
/// in the [`Dialect`] trait. If a field is `None`, the underlying dialect's
/// behavior is used.
#[derive(Debug)]
pub struct DialectOverrides<D: Dialect> {
    dialect: D,

    // Function overrides for character/identifier checking
    is_identifier_start_override: Option<fn(char) -> bool>,
    is_identifier_part_override: Option<fn(char) -> bool>,
    is_delimited_identifier_start_override: Option<fn(char) -> bool>,
    is_custom_operator_part_override: Option<fn(char) -> bool>,

    // Function override for identifier quoting
    identifier_quote_style_override: Option<fn(&str) -> Option<char>>,

    // Slice overrides
    reserved_keywords_for_select_item_operator: Option<&'static [Keyword]>,
    reserved_grantees_types: Option<&'static [GranteesType]>,

    // Precedence overrides
    prec_unknown_override: Option<u8>,

    // Boolean overrides
    supports_string_literal_backslash_escape: Option<bool>,
    ignores_wildcard_escapes: Option<bool>,
    supports_unicode_string_literal: Option<bool>,
    supports_filter_during_aggregation: Option<bool>,
    supports_window_clause_named_window_reference: Option<bool>,
    supports_within_after_array_aggregation: Option<bool>,
    supports_group_by_expr: Option<bool>,
    supports_group_by_with_modifier: Option<bool>,
    supports_left_associative_joins_without_parens: Option<bool>,
    supports_outer_join_operator: Option<bool>,
    supports_cross_join_constraint: Option<bool>,
    supports_connect_by: Option<bool>,
    supports_execute_immediate: Option<bool>,
    supports_match_recognize: Option<bool>,
    supports_in_empty_list: Option<bool>,
    supports_start_transaction_modifier: Option<bool>,
    supports_end_transaction_modifier: Option<bool>,
    supports_named_fn_args_with_eq_operator: Option<bool>,
    supports_named_fn_args_with_colon_operator: Option<bool>,
    supports_named_fn_args_with_assignment_operator: Option<bool>,
    supports_named_fn_args_with_rarrow_operator: Option<bool>,
    supports_named_fn_args_with_expr_name: Option<bool>,
    supports_numeric_prefix: Option<bool>,
    supports_numeric_literal_underscores: Option<bool>,
    supports_window_function_null_treatment_arg: Option<bool>,
    supports_dictionary_syntax: Option<bool>,
    support_map_literal_syntax: Option<bool>,
    supports_lambda_functions: Option<bool>,
    supports_parenthesized_set_variables: Option<bool>,
    supports_comma_separated_set_assignments: Option<bool>,
    supports_select_wildcard_except: Option<bool>,
    convert_type_before_value: Option<bool>,
    supports_triple_quoted_string: Option<bool>,
    supports_trailing_commas: Option<bool>,
    supports_limit_comma: Option<bool>,
    supports_string_literal_concatenation: Option<bool>,
    supports_projection_trailing_commas: Option<bool>,
    supports_from_trailing_commas: Option<bool>,
    supports_column_definition_trailing_commas: Option<bool>,
    supports_object_name_double_dot_notation: Option<bool>,
    supports_struct_literal: Option<bool>,
    supports_empty_projections: Option<bool>,
    supports_select_expr_star: Option<bool>,
    supports_from_first_select: Option<bool>,
    supports_pipe_operator: Option<bool>,
    supports_user_host_grantee: Option<bool>,
    supports_match_against: Option<bool>,
    supports_select_wildcard_exclude: Option<bool>,
    supports_select_exclude: Option<bool>,
    supports_create_table_multi_schema_info_sources: Option<bool>,
    describe_requires_table_keyword: Option<bool>,
    allow_extract_custom: Option<bool>,
    allow_extract_single_quotes: Option<bool>,
    supports_dollar_placeholder: Option<bool>,
    supports_create_index_with_clause: Option<bool>,
    require_interval_qualifier: Option<bool>,
    supports_explain_with_utility_options: Option<bool>,
    supports_asc_desc_in_column_definition: Option<bool>,
    supports_factorial_operator: Option<bool>,
    supports_nested_comments: Option<bool>,
    supports_eq_alias_assignment: Option<bool>,
    supports_try_convert: Option<bool>,
    supports_bang_not_operator: Option<bool>,
    supports_listen_notify: Option<bool>,
    supports_load_data: Option<bool>,
    supports_load_extension: Option<bool>,
    supports_top_before_distinct: Option<bool>,
    supports_boolean_literals: Option<bool>,
    supports_show_like_before_in: Option<bool>,
    supports_comment_on: Option<bool>,
    supports_create_table_select: Option<bool>,
    supports_partiql: Option<bool>,
    supports_table_sample_before_alias: Option<bool>,
    supports_insert_set: Option<bool>,
    supports_insert_table_function: Option<bool>,
    supports_insert_format: Option<bool>,
    supports_set_stmt_without_operator: Option<bool>,
    supports_timestamp_versioning: Option<bool>,
    supports_string_escape_constant: Option<bool>,
    supports_table_hints: Option<bool>,
    requires_single_line_comment_whitespace: Option<bool>,
    supports_array_typedef_with_brackets: Option<bool>,
    supports_geometric_types: Option<bool>,
    supports_order_by_all: Option<bool>,
    supports_set_names: Option<bool>,
    supports_space_separated_column_options: Option<bool>,
    supports_alter_column_type_using: Option<bool>,
    supports_comma_separated_drop_column_list: Option<bool>,
    supports_notnull_operator: Option<bool>,
    supports_data_type_signed_suffix: Option<bool>,
    supports_interval_options: Option<bool>,
    supports_create_table_like_parenthesized: Option<bool>,
    supports_semantic_view_table_factor: Option<bool>,
}

impl<D: Dialect> DialectOverrides<D> {
    /// Create a new `DialectOverrides` wrapping the given dialect.
    pub fn new(dialect: D) -> Self {
        Self {
            dialect,
            is_identifier_start_override: None,
            is_identifier_part_override: None,
            is_delimited_identifier_start_override: None,
            is_custom_operator_part_override: None,
            identifier_quote_style_override: None,
            reserved_keywords_for_select_item_operator: None,
            reserved_grantees_types: None,
            prec_unknown_override: None,
            supports_string_literal_backslash_escape: None,
            ignores_wildcard_escapes: None,
            supports_unicode_string_literal: None,
            supports_filter_during_aggregation: None,
            supports_window_clause_named_window_reference: None,
            supports_within_after_array_aggregation: None,
            supports_group_by_expr: None,
            supports_group_by_with_modifier: None,
            supports_left_associative_joins_without_parens: None,
            supports_outer_join_operator: None,
            supports_cross_join_constraint: None,
            supports_connect_by: None,
            supports_execute_immediate: None,
            supports_match_recognize: None,
            supports_in_empty_list: None,
            supports_start_transaction_modifier: None,
            supports_end_transaction_modifier: None,
            supports_named_fn_args_with_eq_operator: None,
            supports_named_fn_args_with_colon_operator: None,
            supports_named_fn_args_with_assignment_operator: None,
            supports_named_fn_args_with_rarrow_operator: None,
            supports_named_fn_args_with_expr_name: None,
            supports_numeric_prefix: None,
            supports_numeric_literal_underscores: None,
            supports_window_function_null_treatment_arg: None,
            supports_dictionary_syntax: None,
            support_map_literal_syntax: None,
            supports_lambda_functions: None,
            supports_parenthesized_set_variables: None,
            supports_comma_separated_set_assignments: None,
            supports_select_wildcard_except: None,
            convert_type_before_value: None,
            supports_triple_quoted_string: None,
            supports_trailing_commas: None,
            supports_limit_comma: None,
            supports_string_literal_concatenation: None,
            supports_projection_trailing_commas: None,
            supports_from_trailing_commas: None,
            supports_column_definition_trailing_commas: None,
            supports_object_name_double_dot_notation: None,
            supports_struct_literal: None,
            supports_empty_projections: None,
            supports_select_expr_star: None,
            supports_from_first_select: None,
            supports_pipe_operator: None,
            supports_user_host_grantee: None,
            supports_match_against: None,
            supports_select_wildcard_exclude: None,
            supports_select_exclude: None,
            supports_create_table_multi_schema_info_sources: None,
            describe_requires_table_keyword: None,
            allow_extract_custom: None,
            allow_extract_single_quotes: None,
            supports_dollar_placeholder: None,
            supports_create_index_with_clause: None,
            require_interval_qualifier: None,
            supports_explain_with_utility_options: None,
            supports_asc_desc_in_column_definition: None,
            supports_factorial_operator: None,
            supports_nested_comments: None,
            supports_eq_alias_assignment: None,
            supports_try_convert: None,
            supports_bang_not_operator: None,
            supports_listen_notify: None,
            supports_load_data: None,
            supports_load_extension: None,
            supports_top_before_distinct: None,
            supports_boolean_literals: None,
            supports_show_like_before_in: None,
            supports_comment_on: None,
            supports_create_table_select: None,
            supports_partiql: None,
            supports_table_sample_before_alias: None,
            supports_insert_set: None,
            supports_insert_table_function: None,
            supports_insert_format: None,
            supports_set_stmt_without_operator: None,
            supports_timestamp_versioning: None,
            supports_string_escape_constant: None,
            supports_table_hints: None,
            requires_single_line_comment_whitespace: None,
            supports_array_typedef_with_brackets: None,
            supports_geometric_types: None,
            supports_order_by_all: None,
            supports_set_names: None,
            supports_space_separated_column_options: None,
            supports_alter_column_type_using: None,
            supports_comma_separated_drop_column_list: None,
            supports_notnull_operator: None,
            supports_data_type_signed_suffix: None,
            supports_interval_options: None,
            supports_create_table_like_parenthesized: None,
            supports_semantic_view_table_factor: None,
        }
    }

    /// Get a reference to the underlying dialect.
    pub fn inner(&self) -> &D {
        &self.dialect
    }

    // Builder methods for function overrides

    /// Override the `is_identifier_start` method with a custom function.
    ///
    /// # Example
    /// ```
    /// use sqlparser::dialect::{GenericDialect, DialectOverrides};
    ///
    /// let dialect = DialectOverrides::new(GenericDialect {})
    ///     .with_is_identifier_start(|ch| ch.is_alphabetic() || ch == '_' || ch == '$');
    /// ```
    pub fn with_is_identifier_start(mut self, f: fn(char) -> bool) -> Self {
        self.is_identifier_start_override = Some(f);
        self
    }

    /// Override the `is_identifier_part` method with a custom function.
    pub fn with_is_identifier_part(mut self, f: fn(char) -> bool) -> Self {
        self.is_identifier_part_override = Some(f);
        self
    }

    /// Override the `is_delimited_identifier_start` method with a custom function.
    pub fn with_is_delimited_identifier_start(mut self, f: fn(char) -> bool) -> Self {
        self.is_delimited_identifier_start_override = Some(f);
        self
    }

    /// Override the `is_custom_operator_part` method with a custom function.
    pub fn with_is_custom_operator_part(mut self, f: fn(char) -> bool) -> Self {
        self.is_custom_operator_part_override = Some(f);
        self
    }

    /// Override the `identifier_quote_style` method with a custom function.
    ///
    /// # Example
    /// ```
    /// use sqlparser::dialect::{GenericDialect, DialectOverrides};
    ///
    /// let dialect = DialectOverrides::new(GenericDialect {})
    ///     .with_identifier_quote_style(|_id| Some('`'));
    /// ```
    pub fn with_identifier_quote_style(mut self, f: fn(&str) -> Option<char>) -> Self {
        self.identifier_quote_style_override = Some(f);
        self
    }

    /// Override the `get_reserved_keywords_for_select_item_operator` method.
    pub fn with_reserved_keywords_for_select_item_operator(
        mut self,
        keywords: &'static [Keyword],
    ) -> Self {
        self.reserved_keywords_for_select_item_operator = Some(keywords);
        self
    }

    /// Override the `get_reserved_grantees_types` method.
    pub fn with_reserved_grantees_types(mut self, types: &'static [GranteesType]) -> Self {
        self.reserved_grantees_types = Some(types);
        self
    }

    /// Override the `prec_unknown` method.
    pub fn with_prec_unknown(mut self, value: u8) -> Self {
        self.prec_unknown_override = Some(value);
        self
    }

    // Builder methods for all boolean dialect features

    pub fn supports_string_literal_backslash_escape(mut self, value: bool) -> Self {
        self.supports_string_literal_backslash_escape = Some(value);
        self
    }

    pub fn ignores_wildcard_escapes(mut self, value: bool) -> Self {
        self.ignores_wildcard_escapes = Some(value);
        self
    }

    pub fn supports_unicode_string_literal(mut self, value: bool) -> Self {
        self.supports_unicode_string_literal = Some(value);
        self
    }

    pub fn supports_filter_during_aggregation(mut self, value: bool) -> Self {
        self.supports_filter_during_aggregation = Some(value);
        self
    }

    pub fn supports_window_clause_named_window_reference(mut self, value: bool) -> Self {
        self.supports_window_clause_named_window_reference = Some(value);
        self
    }

    pub fn supports_within_after_array_aggregation(mut self, value: bool) -> Self {
        self.supports_within_after_array_aggregation = Some(value);
        self
    }

    pub fn supports_group_by_expr(mut self, value: bool) -> Self {
        self.supports_group_by_expr = Some(value);
        self
    }

    pub fn supports_group_by_with_modifier(mut self, value: bool) -> Self {
        self.supports_group_by_with_modifier = Some(value);
        self
    }

    pub fn supports_left_associative_joins_without_parens(mut self, value: bool) -> Self {
        self.supports_left_associative_joins_without_parens = Some(value);
        self
    }

    pub fn supports_outer_join_operator(mut self, value: bool) -> Self {
        self.supports_outer_join_operator = Some(value);
        self
    }

    pub fn supports_cross_join_constraint(mut self, value: bool) -> Self {
        self.supports_cross_join_constraint = Some(value);
        self
    }

    pub fn supports_connect_by(mut self, value: bool) -> Self {
        self.supports_connect_by = Some(value);
        self
    }

    pub fn supports_execute_immediate(mut self, value: bool) -> Self {
        self.supports_execute_immediate = Some(value);
        self
    }

    pub fn supports_match_recognize(mut self, value: bool) -> Self {
        self.supports_match_recognize = Some(value);
        self
    }

    pub fn supports_in_empty_list(mut self, value: bool) -> Self {
        self.supports_in_empty_list = Some(value);
        self
    }

    pub fn supports_start_transaction_modifier(mut self, value: bool) -> Self {
        self.supports_start_transaction_modifier = Some(value);
        self
    }

    pub fn supports_end_transaction_modifier(mut self, value: bool) -> Self {
        self.supports_end_transaction_modifier = Some(value);
        self
    }

    pub fn supports_named_fn_args_with_eq_operator(mut self, value: bool) -> Self {
        self.supports_named_fn_args_with_eq_operator = Some(value);
        self
    }

    pub fn supports_named_fn_args_with_colon_operator(mut self, value: bool) -> Self {
        self.supports_named_fn_args_with_colon_operator = Some(value);
        self
    }

    pub fn supports_named_fn_args_with_assignment_operator(mut self, value: bool) -> Self {
        self.supports_named_fn_args_with_assignment_operator = Some(value);
        self
    }

    pub fn supports_named_fn_args_with_rarrow_operator(mut self, value: bool) -> Self {
        self.supports_named_fn_args_with_rarrow_operator = Some(value);
        self
    }

    pub fn supports_named_fn_args_with_expr_name(mut self, value: bool) -> Self {
        self.supports_named_fn_args_with_expr_name = Some(value);
        self
    }

    pub fn supports_numeric_prefix(mut self, value: bool) -> Self {
        self.supports_numeric_prefix = Some(value);
        self
    }

    pub fn supports_numeric_literal_underscores(mut self, value: bool) -> Self {
        self.supports_numeric_literal_underscores = Some(value);
        self
    }

    pub fn supports_window_function_null_treatment_arg(mut self, value: bool) -> Self {
        self.supports_window_function_null_treatment_arg = Some(value);
        self
    }

    pub fn supports_dictionary_syntax(mut self, value: bool) -> Self {
        self.supports_dictionary_syntax = Some(value);
        self
    }

    pub fn support_map_literal_syntax(mut self, value: bool) -> Self {
        self.support_map_literal_syntax = Some(value);
        self
    }

    pub fn supports_lambda_functions(mut self, value: bool) -> Self {
        self.supports_lambda_functions = Some(value);
        self
    }

    pub fn supports_parenthesized_set_variables(mut self, value: bool) -> Self {
        self.supports_parenthesized_set_variables = Some(value);
        self
    }

    pub fn supports_comma_separated_set_assignments(mut self, value: bool) -> Self {
        self.supports_comma_separated_set_assignments = Some(value);
        self
    }

    pub fn supports_select_wildcard_except(mut self, value: bool) -> Self {
        self.supports_select_wildcard_except = Some(value);
        self
    }

    pub fn convert_type_before_value(mut self, value: bool) -> Self {
        self.convert_type_before_value = Some(value);
        self
    }

    pub fn supports_triple_quoted_string(mut self, value: bool) -> Self {
        self.supports_triple_quoted_string = Some(value);
        self
    }

    pub fn supports_trailing_commas(mut self, value: bool) -> Self {
        self.supports_trailing_commas = Some(value);
        self
    }

    pub fn supports_limit_comma(mut self, value: bool) -> Self {
        self.supports_limit_comma = Some(value);
        self
    }

    pub fn supports_string_literal_concatenation(mut self, value: bool) -> Self {
        self.supports_string_literal_concatenation = Some(value);
        self
    }

    pub fn supports_projection_trailing_commas(mut self, value: bool) -> Self {
        self.supports_projection_trailing_commas = Some(value);
        self
    }

    pub fn supports_from_trailing_commas(mut self, value: bool) -> Self {
        self.supports_from_trailing_commas = Some(value);
        self
    }

    pub fn supports_column_definition_trailing_commas(mut self, value: bool) -> Self {
        self.supports_column_definition_trailing_commas = Some(value);
        self
    }

    pub fn supports_object_name_double_dot_notation(mut self, value: bool) -> Self {
        self.supports_object_name_double_dot_notation = Some(value);
        self
    }

    pub fn supports_struct_literal(mut self, value: bool) -> Self {
        self.supports_struct_literal = Some(value);
        self
    }

    pub fn supports_empty_projections(mut self, value: bool) -> Self {
        self.supports_empty_projections = Some(value);
        self
    }

    pub fn supports_select_expr_star(mut self, value: bool) -> Self {
        self.supports_select_expr_star = Some(value);
        self
    }

    pub fn supports_from_first_select(mut self, value: bool) -> Self {
        self.supports_from_first_select = Some(value);
        self
    }

    pub fn supports_pipe_operator(mut self, value: bool) -> Self {
        self.supports_pipe_operator = Some(value);
        self
    }

    pub fn supports_user_host_grantee(mut self, value: bool) -> Self {
        self.supports_user_host_grantee = Some(value);
        self
    }

    pub fn supports_match_against(mut self, value: bool) -> Self {
        self.supports_match_against = Some(value);
        self
    }

    pub fn supports_select_wildcard_exclude(mut self, value: bool) -> Self {
        self.supports_select_wildcard_exclude = Some(value);
        self
    }

    pub fn supports_select_exclude(mut self, value: bool) -> Self {
        self.supports_select_exclude = Some(value);
        self
    }

    pub fn supports_create_table_multi_schema_info_sources(mut self, value: bool) -> Self {
        self.supports_create_table_multi_schema_info_sources = Some(value);
        self
    }

    pub fn describe_requires_table_keyword(mut self, value: bool) -> Self {
        self.describe_requires_table_keyword = Some(value);
        self
    }

    pub fn allow_extract_custom(mut self, value: bool) -> Self {
        self.allow_extract_custom = Some(value);
        self
    }

    pub fn allow_extract_single_quotes(mut self, value: bool) -> Self {
        self.allow_extract_single_quotes = Some(value);
        self
    }

    pub fn supports_dollar_placeholder(mut self, value: bool) -> Self {
        self.supports_dollar_placeholder = Some(value);
        self
    }

    pub fn supports_create_index_with_clause(mut self, value: bool) -> Self {
        self.supports_create_index_with_clause = Some(value);
        self
    }

    pub fn require_interval_qualifier(mut self, value: bool) -> Self {
        self.require_interval_qualifier = Some(value);
        self
    }

    pub fn supports_explain_with_utility_options(mut self, value: bool) -> Self {
        self.supports_explain_with_utility_options = Some(value);
        self
    }

    pub fn supports_asc_desc_in_column_definition(mut self, value: bool) -> Self {
        self.supports_asc_desc_in_column_definition = Some(value);
        self
    }

    pub fn supports_factorial_operator(mut self, value: bool) -> Self {
        self.supports_factorial_operator = Some(value);
        self
    }

    pub fn supports_nested_comments(mut self, value: bool) -> Self {
        self.supports_nested_comments = Some(value);
        self
    }

    pub fn supports_eq_alias_assignment(mut self, value: bool) -> Self {
        self.supports_eq_alias_assignment = Some(value);
        self
    }

    pub fn supports_try_convert(mut self, value: bool) -> Self {
        self.supports_try_convert = Some(value);
        self
    }

    pub fn supports_bang_not_operator(mut self, value: bool) -> Self {
        self.supports_bang_not_operator = Some(value);
        self
    }

    pub fn supports_listen_notify(mut self, value: bool) -> Self {
        self.supports_listen_notify = Some(value);
        self
    }

    pub fn supports_load_data(mut self, value: bool) -> Self {
        self.supports_load_data = Some(value);
        self
    }

    pub fn supports_load_extension(mut self, value: bool) -> Self {
        self.supports_load_extension = Some(value);
        self
    }

    pub fn supports_top_before_distinct(mut self, value: bool) -> Self {
        self.supports_top_before_distinct = Some(value);
        self
    }

    pub fn supports_boolean_literals(mut self, value: bool) -> Self {
        self.supports_boolean_literals = Some(value);
        self
    }

    pub fn supports_show_like_before_in(mut self, value: bool) -> Self {
        self.supports_show_like_before_in = Some(value);
        self
    }

    pub fn supports_comment_on(mut self, value: bool) -> Self {
        self.supports_comment_on = Some(value);
        self
    }

    pub fn supports_create_table_select(mut self, value: bool) -> Self {
        self.supports_create_table_select = Some(value);
        self
    }

    pub fn supports_partiql(mut self, value: bool) -> Self {
        self.supports_partiql = Some(value);
        self
    }

    pub fn supports_table_sample_before_alias(mut self, value: bool) -> Self {
        self.supports_table_sample_before_alias = Some(value);
        self
    }

    pub fn supports_insert_set(mut self, value: bool) -> Self {
        self.supports_insert_set = Some(value);
        self
    }

    pub fn supports_insert_table_function(mut self, value: bool) -> Self {
        self.supports_insert_table_function = Some(value);
        self
    }

    pub fn supports_insert_format(mut self, value: bool) -> Self {
        self.supports_insert_format = Some(value);
        self
    }

    pub fn supports_set_stmt_without_operator(mut self, value: bool) -> Self {
        self.supports_set_stmt_without_operator = Some(value);
        self
    }

    pub fn supports_timestamp_versioning(mut self, value: bool) -> Self {
        self.supports_timestamp_versioning = Some(value);
        self
    }

    pub fn supports_string_escape_constant(mut self, value: bool) -> Self {
        self.supports_string_escape_constant = Some(value);
        self
    }

    pub fn supports_table_hints(mut self, value: bool) -> Self {
        self.supports_table_hints = Some(value);
        self
    }

    pub fn requires_single_line_comment_whitespace(mut self, value: bool) -> Self {
        self.requires_single_line_comment_whitespace = Some(value);
        self
    }

    pub fn supports_array_typedef_with_brackets(mut self, value: bool) -> Self {
        self.supports_array_typedef_with_brackets = Some(value);
        self
    }

    pub fn supports_geometric_types(mut self, value: bool) -> Self {
        self.supports_geometric_types = Some(value);
        self
    }

    pub fn supports_order_by_all(mut self, value: bool) -> Self {
        self.supports_order_by_all = Some(value);
        self
    }

    pub fn supports_set_names(mut self, value: bool) -> Self {
        self.supports_set_names = Some(value);
        self
    }

    pub fn supports_space_separated_column_options(mut self, value: bool) -> Self {
        self.supports_space_separated_column_options = Some(value);
        self
    }

    pub fn supports_alter_column_type_using(mut self, value: bool) -> Self {
        self.supports_alter_column_type_using = Some(value);
        self
    }

    pub fn supports_comma_separated_drop_column_list(mut self, value: bool) -> Self {
        self.supports_comma_separated_drop_column_list = Some(value);
        self
    }

    pub fn supports_notnull_operator(mut self, value: bool) -> Self {
        self.supports_notnull_operator = Some(value);
        self
    }

    pub fn supports_data_type_signed_suffix(mut self, value: bool) -> Self {
        self.supports_data_type_signed_suffix = Some(value);
        self
    }

    pub fn supports_interval_options(mut self, value: bool) -> Self {
        self.supports_interval_options = Some(value);
        self
    }

    pub fn supports_create_table_like_parenthesized(mut self, value: bool) -> Self {
        self.supports_create_table_like_parenthesized = Some(value);
        self
    }

    pub fn supports_semantic_view_table_factor(mut self, value: bool) -> Self {
        self.supports_semantic_view_table_factor = Some(value);
        self
    }
}

impl<D: Dialect> Dialect for DialectOverrides<D> {
    fn dialect(&self) -> TypeId {
        self.dialect.dialect()
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        self.is_delimited_identifier_start_override
            .map(|f| f(ch))
            .unwrap_or_else(|| self.dialect.is_delimited_identifier_start(ch))
    }

    fn is_nested_delimited_identifier_start(&self, ch: char) -> bool {
        self.dialect.is_nested_delimited_identifier_start(ch)
    }

    fn peek_nested_delimited_identifier_quotes(
        &self,
        chars: Peekable<Chars<'_>>,
    ) -> Option<(char, Option<char>)> {
        self.dialect.peek_nested_delimited_identifier_quotes(chars)
    }

    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        self.identifier_quote_style_override
            .map(|f| f(identifier))
            .unwrap_or_else(|| self.dialect.identifier_quote_style(identifier))
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        self.is_identifier_start_override
            .map(|f| f(ch))
            .unwrap_or_else(|| self.dialect.is_identifier_start(ch))
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.is_identifier_part_override
            .map(|f| f(ch))
            .unwrap_or_else(|| self.dialect.is_identifier_part(ch))
    }

    fn is_custom_operator_part(&self, ch: char) -> bool {
        self.is_custom_operator_part_override
            .map(|f| f(ch))
            .unwrap_or_else(|| self.dialect.is_custom_operator_part(ch))
    }

    fn supports_string_literal_backslash_escape(&self) -> bool {
        self.supports_string_literal_backslash_escape
            .unwrap_or_else(|| self.dialect.supports_string_literal_backslash_escape())
    }

    fn ignores_wildcard_escapes(&self) -> bool {
        self.ignores_wildcard_escapes
            .unwrap_or_else(|| self.dialect.ignores_wildcard_escapes())
    }

    fn supports_unicode_string_literal(&self) -> bool {
        self.supports_unicode_string_literal
            .unwrap_or_else(|| self.dialect.supports_unicode_string_literal())
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        self.supports_filter_during_aggregation
            .unwrap_or_else(|| self.dialect.supports_filter_during_aggregation())
    }

    fn supports_window_clause_named_window_reference(&self) -> bool {
        self.supports_window_clause_named_window_reference
            .unwrap_or_else(|| self.dialect.supports_window_clause_named_window_reference())
    }

    fn supports_within_after_array_aggregation(&self) -> bool {
        self.supports_within_after_array_aggregation
            .unwrap_or_else(|| self.dialect.supports_within_after_array_aggregation())
    }

    fn supports_group_by_expr(&self) -> bool {
        self.supports_group_by_expr
            .unwrap_or_else(|| self.dialect.supports_group_by_expr())
    }

    fn supports_group_by_with_modifier(&self) -> bool {
        self.supports_group_by_with_modifier
            .unwrap_or_else(|| self.dialect.supports_group_by_with_modifier())
    }

    fn supports_left_associative_joins_without_parens(&self) -> bool {
        self.supports_left_associative_joins_without_parens
            .unwrap_or_else(|| {
                self.dialect
                    .supports_left_associative_joins_without_parens()
            })
    }

    fn supports_outer_join_operator(&self) -> bool {
        self.supports_outer_join_operator
            .unwrap_or_else(|| self.dialect.supports_outer_join_operator())
    }

    fn supports_cross_join_constraint(&self) -> bool {
        self.supports_cross_join_constraint
            .unwrap_or_else(|| self.dialect.supports_cross_join_constraint())
    }

    fn supports_connect_by(&self) -> bool {
        self.supports_connect_by
            .unwrap_or_else(|| self.dialect.supports_connect_by())
    }

    fn supports_execute_immediate(&self) -> bool {
        self.supports_execute_immediate
            .unwrap_or_else(|| self.dialect.supports_execute_immediate())
    }

    fn supports_match_recognize(&self) -> bool {
        self.supports_match_recognize
            .unwrap_or_else(|| self.dialect.supports_match_recognize())
    }

    fn supports_in_empty_list(&self) -> bool {
        self.supports_in_empty_list
            .unwrap_or_else(|| self.dialect.supports_in_empty_list())
    }

    fn supports_start_transaction_modifier(&self) -> bool {
        self.supports_start_transaction_modifier
            .unwrap_or_else(|| self.dialect.supports_start_transaction_modifier())
    }

    fn supports_end_transaction_modifier(&self) -> bool {
        self.supports_end_transaction_modifier
            .unwrap_or_else(|| self.dialect.supports_end_transaction_modifier())
    }

    fn supports_named_fn_args_with_eq_operator(&self) -> bool {
        self.supports_named_fn_args_with_eq_operator
            .unwrap_or_else(|| self.dialect.supports_named_fn_args_with_eq_operator())
    }

    fn supports_named_fn_args_with_colon_operator(&self) -> bool {
        self.supports_named_fn_args_with_colon_operator
            .unwrap_or_else(|| self.dialect.supports_named_fn_args_with_colon_operator())
    }

    fn supports_named_fn_args_with_assignment_operator(&self) -> bool {
        self.supports_named_fn_args_with_assignment_operator
            .unwrap_or_else(|| {
                self.dialect
                    .supports_named_fn_args_with_assignment_operator()
            })
    }

    fn supports_named_fn_args_with_rarrow_operator(&self) -> bool {
        self.supports_named_fn_args_with_rarrow_operator
            .unwrap_or_else(|| self.dialect.supports_named_fn_args_with_rarrow_operator())
    }

    fn supports_named_fn_args_with_expr_name(&self) -> bool {
        self.supports_named_fn_args_with_expr_name
            .unwrap_or_else(|| self.dialect.supports_named_fn_args_with_expr_name())
    }

    fn supports_numeric_prefix(&self) -> bool {
        self.supports_numeric_prefix
            .unwrap_or_else(|| self.dialect.supports_numeric_prefix())
    }

    fn supports_numeric_literal_underscores(&self) -> bool {
        self.supports_numeric_literal_underscores
            .unwrap_or_else(|| self.dialect.supports_numeric_literal_underscores())
    }

    fn supports_window_function_null_treatment_arg(&self) -> bool {
        self.supports_window_function_null_treatment_arg
            .unwrap_or_else(|| self.dialect.supports_window_function_null_treatment_arg())
    }

    fn supports_dictionary_syntax(&self) -> bool {
        self.supports_dictionary_syntax
            .unwrap_or_else(|| self.dialect.supports_dictionary_syntax())
    }

    fn support_map_literal_syntax(&self) -> bool {
        self.support_map_literal_syntax
            .unwrap_or_else(|| self.dialect.support_map_literal_syntax())
    }

    fn supports_lambda_functions(&self) -> bool {
        self.supports_lambda_functions
            .unwrap_or_else(|| self.dialect.supports_lambda_functions())
    }

    fn supports_parenthesized_set_variables(&self) -> bool {
        self.supports_parenthesized_set_variables
            .unwrap_or_else(|| self.dialect.supports_parenthesized_set_variables())
    }

    fn supports_comma_separated_set_assignments(&self) -> bool {
        self.supports_comma_separated_set_assignments
            .unwrap_or_else(|| self.dialect.supports_comma_separated_set_assignments())
    }

    fn supports_select_wildcard_except(&self) -> bool {
        self.supports_select_wildcard_except
            .unwrap_or_else(|| self.dialect.supports_select_wildcard_except())
    }

    fn convert_type_before_value(&self) -> bool {
        self.convert_type_before_value
            .unwrap_or_else(|| self.dialect.convert_type_before_value())
    }

    fn supports_triple_quoted_string(&self) -> bool {
        self.supports_triple_quoted_string
            .unwrap_or_else(|| self.dialect.supports_triple_quoted_string())
    }

    fn parse_prefix(&self, parser: &mut Parser) -> Option<Result<Expr, ParserError>> {
        self.dialect.parse_prefix(parser)
    }

    fn supports_trailing_commas(&self) -> bool {
        self.supports_trailing_commas
            .unwrap_or_else(|| self.dialect.supports_trailing_commas())
    }

    fn supports_limit_comma(&self) -> bool {
        self.supports_limit_comma
            .unwrap_or_else(|| self.dialect.supports_limit_comma())
    }

    fn supports_string_literal_concatenation(&self) -> bool {
        self.supports_string_literal_concatenation
            .unwrap_or_else(|| self.dialect.supports_string_literal_concatenation())
    }

    fn supports_projection_trailing_commas(&self) -> bool {
        self.supports_projection_trailing_commas
            .unwrap_or_else(|| self.dialect.supports_projection_trailing_commas())
    }

    fn supports_from_trailing_commas(&self) -> bool {
        self.supports_from_trailing_commas
            .unwrap_or_else(|| self.dialect.supports_from_trailing_commas())
    }

    fn supports_column_definition_trailing_commas(&self) -> bool {
        self.supports_column_definition_trailing_commas
            .unwrap_or_else(|| self.dialect.supports_column_definition_trailing_commas())
    }

    fn supports_object_name_double_dot_notation(&self) -> bool {
        self.supports_object_name_double_dot_notation
            .unwrap_or_else(|| self.dialect.supports_object_name_double_dot_notation())
    }

    fn supports_struct_literal(&self) -> bool {
        self.supports_struct_literal
            .unwrap_or_else(|| self.dialect.supports_struct_literal())
    }

    fn supports_empty_projections(&self) -> bool {
        self.supports_empty_projections
            .unwrap_or_else(|| self.dialect.supports_empty_projections())
    }

    fn supports_select_expr_star(&self) -> bool {
        self.supports_select_expr_star
            .unwrap_or_else(|| self.dialect.supports_select_expr_star())
    }

    fn supports_from_first_select(&self) -> bool {
        self.supports_from_first_select
            .unwrap_or_else(|| self.dialect.supports_from_first_select())
    }

    fn supports_pipe_operator(&self) -> bool {
        self.supports_pipe_operator
            .unwrap_or_else(|| self.dialect.supports_pipe_operator())
    }

    fn supports_user_host_grantee(&self) -> bool {
        self.supports_user_host_grantee
            .unwrap_or_else(|| self.dialect.supports_user_host_grantee())
    }

    fn supports_match_against(&self) -> bool {
        self.supports_match_against
            .unwrap_or_else(|| self.dialect.supports_match_against())
    }

    fn supports_select_wildcard_exclude(&self) -> bool {
        self.supports_select_wildcard_exclude
            .unwrap_or_else(|| self.dialect.supports_select_wildcard_exclude())
    }

    fn supports_select_exclude(&self) -> bool {
        self.supports_select_exclude
            .unwrap_or_else(|| self.dialect.supports_select_exclude())
    }

    fn supports_create_table_multi_schema_info_sources(&self) -> bool {
        self.supports_create_table_multi_schema_info_sources
            .unwrap_or_else(|| {
                self.dialect
                    .supports_create_table_multi_schema_info_sources()
            })
    }

    fn parse_infix(
        &self,
        parser: &mut Parser,
        expr: &Expr,
        precedence: u8,
    ) -> Option<Result<Expr, ParserError>> {
        self.dialect.parse_infix(parser, expr, precedence)
    }

    fn get_next_precedence(&self, parser: &Parser) -> Option<Result<u8, ParserError>> {
        self.dialect.get_next_precedence(parser)
    }

    fn parse_statement(&self, parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
        self.dialect.parse_statement(parser)
    }

    fn parse_column_option(
        &self,
        parser: &mut Parser,
    ) -> Result<Option<Result<Option<ColumnOption>, ParserError>>, ParserError> {
        self.dialect.parse_column_option(parser)
    }

    fn describe_requires_table_keyword(&self) -> bool {
        self.describe_requires_table_keyword
            .unwrap_or_else(|| self.dialect.describe_requires_table_keyword())
    }

    fn allow_extract_custom(&self) -> bool {
        self.allow_extract_custom
            .unwrap_or_else(|| self.dialect.allow_extract_custom())
    }

    fn allow_extract_single_quotes(&self) -> bool {
        self.allow_extract_single_quotes
            .unwrap_or_else(|| self.dialect.allow_extract_single_quotes())
    }

    fn supports_dollar_placeholder(&self) -> bool {
        self.supports_dollar_placeholder
            .unwrap_or_else(|| self.dialect.supports_dollar_placeholder())
    }

    fn supports_create_index_with_clause(&self) -> bool {
        self.supports_create_index_with_clause
            .unwrap_or_else(|| self.dialect.supports_create_index_with_clause())
    }

    fn require_interval_qualifier(&self) -> bool {
        self.require_interval_qualifier
            .unwrap_or_else(|| self.dialect.require_interval_qualifier())
    }

    fn supports_explain_with_utility_options(&self) -> bool {
        self.supports_explain_with_utility_options
            .unwrap_or_else(|| self.dialect.supports_explain_with_utility_options())
    }

    fn supports_asc_desc_in_column_definition(&self) -> bool {
        self.supports_asc_desc_in_column_definition
            .unwrap_or_else(|| self.dialect.supports_asc_desc_in_column_definition())
    }

    fn supports_factorial_operator(&self) -> bool {
        self.supports_factorial_operator
            .unwrap_or_else(|| self.dialect.supports_factorial_operator())
    }

    fn supports_nested_comments(&self) -> bool {
        self.supports_nested_comments
            .unwrap_or_else(|| self.dialect.supports_nested_comments())
    }

    fn supports_eq_alias_assignment(&self) -> bool {
        self.supports_eq_alias_assignment
            .unwrap_or_else(|| self.dialect.supports_eq_alias_assignment())
    }

    fn supports_try_convert(&self) -> bool {
        self.supports_try_convert
            .unwrap_or_else(|| self.dialect.supports_try_convert())
    }

    fn supports_bang_not_operator(&self) -> bool {
        self.supports_bang_not_operator
            .unwrap_or_else(|| self.dialect.supports_bang_not_operator())
    }

    fn supports_listen_notify(&self) -> bool {
        self.supports_listen_notify
            .unwrap_or_else(|| self.dialect.supports_listen_notify())
    }

    fn supports_load_data(&self) -> bool {
        self.supports_load_data
            .unwrap_or_else(|| self.dialect.supports_load_data())
    }

    fn supports_load_extension(&self) -> bool {
        self.supports_load_extension
            .unwrap_or_else(|| self.dialect.supports_load_extension())
    }

    fn supports_top_before_distinct(&self) -> bool {
        self.supports_top_before_distinct
            .unwrap_or_else(|| self.dialect.supports_top_before_distinct())
    }

    fn supports_boolean_literals(&self) -> bool {
        self.supports_boolean_literals
            .unwrap_or_else(|| self.dialect.supports_boolean_literals())
    }

    fn supports_show_like_before_in(&self) -> bool {
        self.supports_show_like_before_in
            .unwrap_or_else(|| self.dialect.supports_show_like_before_in())
    }

    fn supports_comment_on(&self) -> bool {
        self.supports_comment_on
            .unwrap_or_else(|| self.dialect.supports_comment_on())
    }

    fn supports_create_table_select(&self) -> bool {
        self.supports_create_table_select
            .unwrap_or_else(|| self.dialect.supports_create_table_select())
    }

    fn supports_partiql(&self) -> bool {
        self.supports_partiql
            .unwrap_or_else(|| self.dialect.supports_partiql())
    }

    fn is_reserved_for_identifier(&self, kw: Keyword) -> bool {
        self.dialect.is_reserved_for_identifier(kw)
    }

    fn get_reserved_keywords_for_select_item_operator(&self) -> &[Keyword] {
        self.reserved_keywords_for_select_item_operator
            .unwrap_or_else(|| {
                self.dialect
                    .get_reserved_keywords_for_select_item_operator()
            })
    }

    fn get_reserved_grantees_types(&self) -> &[GranteesType] {
        self.reserved_grantees_types
            .unwrap_or_else(|| self.dialect.get_reserved_grantees_types())
    }

    fn supports_table_sample_before_alias(&self) -> bool {
        self.supports_table_sample_before_alias
            .unwrap_or_else(|| self.dialect.supports_table_sample_before_alias())
    }

    fn supports_insert_set(&self) -> bool {
        self.supports_insert_set
            .unwrap_or_else(|| self.dialect.supports_insert_set())
    }

    fn supports_insert_table_function(&self) -> bool {
        self.supports_insert_table_function
            .unwrap_or_else(|| self.dialect.supports_insert_table_function())
    }

    fn supports_insert_format(&self) -> bool {
        self.supports_insert_format
            .unwrap_or_else(|| self.dialect.supports_insert_format())
    }

    fn supports_set_stmt_without_operator(&self) -> bool {
        self.supports_set_stmt_without_operator
            .unwrap_or_else(|| self.dialect.supports_set_stmt_without_operator())
    }

    fn is_column_alias(&self, kw: &Keyword, parser: &mut Parser) -> bool {
        self.dialect.is_column_alias(kw, parser)
    }

    fn is_select_item_alias(&self, explicit: bool, kw: &Keyword, parser: &mut Parser) -> bool {
        self.dialect.is_select_item_alias(explicit, kw, parser)
    }

    fn is_table_factor(&self, kw: &Keyword, parser: &mut Parser) -> bool {
        self.dialect.is_table_factor(kw, parser)
    }

    fn is_table_alias(&self, kw: &Keyword, parser: &mut Parser) -> bool {
        self.dialect.is_table_alias(kw, parser)
    }

    fn is_table_factor_alias(&self, explicit: bool, kw: &Keyword, parser: &mut Parser) -> bool {
        self.dialect.is_table_factor_alias(explicit, kw, parser)
    }

    fn supports_timestamp_versioning(&self) -> bool {
        self.supports_timestamp_versioning
            .unwrap_or_else(|| self.dialect.supports_timestamp_versioning())
    }

    fn supports_string_escape_constant(&self) -> bool {
        self.supports_string_escape_constant
            .unwrap_or_else(|| self.dialect.supports_string_escape_constant())
    }

    fn supports_table_hints(&self) -> bool {
        self.supports_table_hints
            .unwrap_or_else(|| self.dialect.supports_table_hints())
    }

    fn requires_single_line_comment_whitespace(&self) -> bool {
        self.requires_single_line_comment_whitespace
            .unwrap_or_else(|| self.dialect.requires_single_line_comment_whitespace())
    }

    fn supports_array_typedef_with_brackets(&self) -> bool {
        self.supports_array_typedef_with_brackets
            .unwrap_or_else(|| self.dialect.supports_array_typedef_with_brackets())
    }

    fn supports_geometric_types(&self) -> bool {
        self.supports_geometric_types
            .unwrap_or_else(|| self.dialect.supports_geometric_types())
    }

    fn supports_order_by_all(&self) -> bool {
        self.supports_order_by_all
            .unwrap_or_else(|| self.dialect.supports_order_by_all())
    }

    fn supports_set_names(&self) -> bool {
        self.supports_set_names
            .unwrap_or_else(|| self.dialect.supports_set_names())
    }

    fn supports_space_separated_column_options(&self) -> bool {
        self.supports_space_separated_column_options
            .unwrap_or_else(|| self.dialect.supports_space_separated_column_options())
    }

    fn supports_alter_column_type_using(&self) -> bool {
        self.supports_alter_column_type_using
            .unwrap_or_else(|| self.dialect.supports_alter_column_type_using())
    }

    fn supports_comma_separated_drop_column_list(&self) -> bool {
        self.supports_comma_separated_drop_column_list
            .unwrap_or_else(|| self.dialect.supports_comma_separated_drop_column_list())
    }

    fn is_identifier_generating_function_name(
        &self,
        ident: &Ident,
        name_parts: &[ObjectNamePart],
    ) -> bool {
        self.dialect
            .is_identifier_generating_function_name(ident, name_parts)
    }

    fn supports_notnull_operator(&self) -> bool {
        self.supports_notnull_operator
            .unwrap_or_else(|| self.dialect.supports_notnull_operator())
    }

    fn supports_data_type_signed_suffix(&self) -> bool {
        self.supports_data_type_signed_suffix
            .unwrap_or_else(|| self.dialect.supports_data_type_signed_suffix())
    }

    fn supports_interval_options(&self) -> bool {
        self.supports_interval_options
            .unwrap_or_else(|| self.dialect.supports_interval_options())
    }

    fn supports_create_table_like_parenthesized(&self) -> bool {
        self.supports_create_table_like_parenthesized
            .unwrap_or_else(|| self.dialect.supports_create_table_like_parenthesized())
    }

    fn supports_semantic_view_table_factor(&self) -> bool {
        self.supports_semantic_view_table_factor
            .unwrap_or_else(|| self.dialect.supports_semantic_view_table_factor())
    }

    fn prec_unknown(&self) -> u8 {
        self.prec_unknown_override
            .unwrap_or_else(|| self.dialect.prec_unknown())
    }
}
