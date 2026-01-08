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

//! Tests for the `derive_dialect!` macro

use sqlparser::derive_dialect;
use sqlparser::dialect::{Dialect, DialectOverrides, GenericDialect};
use sqlparser::parser::Parser;

// Test basic macro usage
derive_dialect!(MyCustomDialect, GenericDialect);

#[test]
fn test_derive_dialect_basic() {
    let dialect = MyCustomDialect::new();

    // Should work as a dialect
    assert!(dialect.is_identifier_start('a'));
    assert!(dialect.is_identifier_start('_'));
    assert!(dialect.is_identifier_part('1'));
}

#[test]
fn test_derive_dialect_has_unique_type_id() {
    let custom = MyCustomDialect::new();
    let generic = GenericDialect;

    struct ParserLike<'a> {
        dialect: &'a dyn Dialect,
    }

    macro_rules! dialect_of {
        ( $parsed_dialect: ident is $($dialect_type: ty)|+ ) => {
            ($($parsed_dialect.dialect.is::<$dialect_type>())||+)
        };
    }

    let custom_parser = ParserLike { dialect: &custom };
    let generic_parser = ParserLike { dialect: &generic };

    // Custom dialect should have its own type
    assert!(dialect_of!(custom_parser is MyCustomDialect));
    assert!(!dialect_of!(custom_parser is GenericDialect));

    // Generic should not match custom
    assert!(dialect_of!(generic_parser is GenericDialect));
    assert!(!dialect_of!(generic_parser is MyCustomDialect));
}

#[test]
fn test_derive_dialect_with_overrides() {
    let dialect = DialectOverrides::new(MyCustomDialect::new())
        .supports_order_by_all(true)
        .supports_nested_comments(true);

    // Check overrides work
    assert!(Dialect::supports_order_by_all(&dialect));
    assert!(Dialect::supports_nested_comments(&dialect));

    // Type identity should be preserved
    let dialect_ref: &dyn Dialect = &dialect;
    assert!(dialect_ref.is::<MyCustomDialect>());
}

#[test]
fn test_derive_dialect_parsing() {
    let dialect = DialectOverrides::new(MyCustomDialect::new())
        .supports_order_by_all(true);

    let sql = "SELECT * FROM users ORDER BY ALL";
    let result = Parser::new(&dialect)
        .try_with_sql(sql)
        .unwrap()
        .parse_statements();

    assert!(result.is_ok(), "Should parse SQL with custom dialect");
}

// Test with another base dialect
derive_dialect!(MyGeneric2Dialect, GenericDialect);

#[test]
fn test_derive_dialect_second_base() {
    let dialect = MyGeneric2Dialect::new();

    // Should inherit GenericDialect features
    assert!(dialect.is_identifier_start('a'));
    assert!(dialect.is_identifier_start('_'));
}

#[test]
fn test_derive_dialect_with_extensions() {
    let dialect = DialectOverrides::new(MyGeneric2Dialect::new())
        .supports_order_by_all(true)
        .with_is_identifier_start(|ch| ch.is_alphabetic() || ch == '_' || ch == '$');

    // Base features still work
    assert!(dialect.is_identifier_part('1'));

    // Plus our extensions
    assert!(Dialect::supports_order_by_all(&dialect));
    assert!(Dialect::is_identifier_start(&dialect, '$'));
}

// Test with optional method overrides
derive_dialect!(MyDialectWithOverrides, GenericDialect, optional_methods: {
    fn supports_outer_join_operator(&self) -> bool {
        true  // Override to enable this feature
    }

    fn supports_cross_join_constraint(&self) -> bool {
        true  // Override to enable this feature
    }
});

#[test]
fn test_derive_dialect_with_optional_methods() {
    let dialect = MyDialectWithOverrides::new();

    // Should have the overridden methods
    assert!(Dialect::supports_outer_join_operator(&dialect));
    assert!(Dialect::supports_cross_join_constraint(&dialect));

    // Default GenericDialect doesn't have these
    let generic = GenericDialect;
    assert!(!Dialect::supports_outer_join_operator(&generic));
    assert!(!Dialect::supports_cross_join_constraint(&generic));
}

#[test]
fn test_derive_dialect_optional_methods_with_dialect_overrides() {
    let dialect = DialectOverrides::new(MyDialectWithOverrides::new())
        .supports_order_by_all(true)
        .supports_outer_join_operator(false); // Override the override!

    // DialectOverrides should win
    assert!(!Dialect::supports_outer_join_operator(&dialect));
    assert!(Dialect::supports_order_by_all(&dialect));

    // The non-overridden method from the base should still work
    assert!(Dialect::supports_cross_join_constraint(&dialect));
}

// Test multiple custom dialects can coexist
derive_dialect!(AlphaDialect, GenericDialect);
derive_dialect!(BetaDialect, GenericDialect);
derive_dialect!(GammaDialect, GenericDialect);

#[test]
fn test_multiple_derived_dialects() {
    let alpha = AlphaDialect::new();
    let beta = BetaDialect::new();
    let gamma = GammaDialect::new();

    struct ParserLike<'a> {
        dialect: &'a dyn Dialect,
    }

    macro_rules! dialect_of {
        ( $parsed_dialect: ident is $($dialect_type: ty)|+ ) => {
            ($($parsed_dialect.dialect.is::<$dialect_type>())||+)
        };
    }

    let alpha_parser = ParserLike { dialect: &alpha };
    let beta_parser = ParserLike { dialect: &beta };
    let gamma_parser = ParserLike { dialect: &gamma };

    // Each should have its own unique type
    assert!(dialect_of!(alpha_parser is AlphaDialect));
    assert!(!dialect_of!(alpha_parser is BetaDialect));
    assert!(!dialect_of!(alpha_parser is GammaDialect));

    assert!(dialect_of!(beta_parser is BetaDialect));
    assert!(!dialect_of!(beta_parser is AlphaDialect));

    assert!(dialect_of!(gamma_parser is GammaDialect));
    assert!(!dialect_of!(gamma_parser is GenericDialect));
}

#[test]
fn test_derive_dialect_chaining() {
    // You can chain multiple operations on a derived dialect
    let dialect = DialectOverrides::new(MyCustomDialect::new())
        .supports_order_by_all(true)
        .supports_triple_quoted_string(true)
        .supports_filter_during_aggregation(true)
        .with_is_identifier_start(|ch| ch.is_alphabetic() || ch == '_' || ch == '$')
        .with_identifier_quote_style(|_| Some('`'))
        .with_prec_unknown(10);

    // Verify all features work together
    assert!(Dialect::supports_order_by_all(&dialect));
    assert!(Dialect::supports_triple_quoted_string(&dialect));
    assert!(Dialect::supports_filter_during_aggregation(&dialect));
    assert!(Dialect::is_identifier_start(&dialect, '$'));
    assert_eq!(Dialect::identifier_quote_style(&dialect, "test"), Some('`'));
    assert_eq!(Dialect::prec_unknown(&dialect), 10);

    // Type identity preserved
    let dialect_ref: &dyn Dialect = &dialect;
    assert!(dialect_ref.is::<MyCustomDialect>());
}

// Test that the macro works with various naming conventions
derive_dialect!(snake_case_dialect, GenericDialect);
derive_dialect!(SCREAMING_SNAKE, GenericDialect);
derive_dialect!(CamelCaseDialect, GenericDialect);

#[test]
fn test_derive_dialect_naming_conventions() {
    let _snake = snake_case_dialect::new();
    let _screaming = SCREAMING_SNAKE::new();
    let _camel = CamelCaseDialect::new();

    // All should work
    assert!(Dialect::is_identifier_start(&_snake, 'a'));
    assert!(Dialect::is_identifier_start(&_screaming, 'a'));
    assert!(Dialect::is_identifier_start(&_camel, 'a'));
}

// Complex example: a dialect with many custom features
derive_dialect!(AdvancedDialect, GenericDialect, optional_methods: {
    fn supports_order_by_all(&self) -> bool {
        true
    }

    fn supports_triple_quoted_string(&self) -> bool {
        true
    }

    fn supports_dictionary_syntax(&self) -> bool {
        true
    }
});

#[test]
fn test_derive_dialect_advanced() {
    let dialect = AdvancedDialect::new();

    // Should have our custom features
    assert!(Dialect::supports_order_by_all(&dialect));
    assert!(Dialect::supports_triple_quoted_string(&dialect));
    assert!(Dialect::supports_dictionary_syntax(&dialect));

    // Can still use with DialectOverrides for even more customization
    let enhanced = DialectOverrides::new(dialect)
        .with_is_identifier_start(|ch| ch.is_alphabetic() || ch == '_' || ch == '$' || ch == '@')
        .supports_filter_during_aggregation(true);

    assert!(Dialect::is_identifier_start(&enhanced, '@'));
    assert!(Dialect::supports_order_by_all(&enhanced));
    assert!(Dialect::supports_filter_during_aggregation(&enhanced));
}
