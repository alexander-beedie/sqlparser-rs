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

//! Example showing how to create a custom dialect that has a unique TypeId
//! for use with `dialect_of!` checks, while leveraging DialectOverrides
//!
//! # Creating Custom Dialects with Unique Identity
//!
//! When you create a custom dialect struct and implement the `Dialect` trait for it,
//! it automatically gets its own unique `TypeId`. This means that `dialect_of!` checks
//! will see your custom type, not the underlying base dialect.
//!
//! ## Pattern 1: Wrapping an Existing Dialect
//!
//! Create a new struct that wraps an existing dialect and delegates to it:
//!
//! ```rust,ignore
//! #[derive(Debug)]
//! struct CustomDialect {
//!     inner: GenericDialect,
//! }
//!
//! impl Dialect for CustomDialect {
//!     fn is_identifier_start(&self, ch: char) -> bool {
//!         self.inner.is_identifier_start(ch)
//!     }
//!     // ... delegate other required methods
//! }
//! ```
//!
//! Now `dialect_of!(parser is CustomDialect)` will return true, not GenericDialect.
//!
//! ## Pattern 2: Using DialectOverrides for Additional Features
//!
//! After creating your custom dialect, you can wrap it in `DialectOverrides`
//! to add or modify specific behaviors:
//!
//! ```rust,ignore
//! let dialect = DialectOverrides::new(CustomDialect::new())
//!     .supports_order_by_all(true)
//!     .supports_triple_quoted_string(true);
//! ```
//!
//! This gives you:
//! - A unique dialect identity (CustomDialect)
//! - The base dialect's features (from GenericDialect or whatever you wrap)
//! - Additional override capabilities (from DialectOverrides)
//!
//! ## Pattern 3: Hybrid Dialects
//!
//! You can create dialects that combine features from multiple SQL dialects:
//!
//! ```rust,ignore
//! impl Dialect for HybridDialect {
//!     fn supports_filter_during_aggregation(&self) -> bool {
//!         true // PostgreSQL-like
//!     }
//!
//!     fn supports_string_literal_backslash_escape(&self) -> bool {
//!         true // MySQL-like
//!     }
//! }
//! ```

use sqlparser::dialect::{Dialect, DialectOverrides, GenericDialect};
use sqlparser::parser::Parser;

/// A custom dialect that wraps GenericDialect but has its own unique TypeId
#[derive(Debug)]
struct CustomDialect {
    inner: GenericDialect,
}

impl CustomDialect {
    fn new() -> Self {
        Self {
            inner: GenericDialect {},
        }
    }
}

// Implement Dialect by delegating to the inner GenericDialect
impl Dialect for CustomDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        self.inner.is_identifier_start(ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.inner.is_identifier_part(ch)
    }

    // Note: We don't override dialect() method, so it uses the default
    // implementation which returns TypeId::of::<CustomDialect>()
}

#[test]
fn test_custom_dialect_with_overrides() {
    // Create a custom dialect and then use DialectOverrides to add features
    let custom = CustomDialect::new();
    let dialect = DialectOverrides::new(custom).supports_order_by_all(true);

    // Helper to test dialect_of!
    struct ParserLike<'a> {
        dialect: &'a dyn Dialect,
    }

    macro_rules! dialect_of {
        ( $parsed_dialect: ident is $($dialect_type: ty)|+ ) => {
            ($($parsed_dialect.dialect.is::<$dialect_type>())||+)
        };
    }

    let parser = ParserLike { dialect: &dialect };

    // Now dialect_of! will see CustomDialect, not GenericDialect
    assert!(dialect_of!(parser is CustomDialect));
    assert!(!dialect_of!(parser is GenericDialect));

    // But we still get the benefits of DialectOverrides
    assert!(Dialect::supports_order_by_all(&dialect));
}

#[test]
fn test_custom_dialect_parsing() {
    let custom = CustomDialect::new();
    let dialect = DialectOverrides::new(custom)
        .supports_order_by_all(true)
        .supports_triple_quoted_string(true);

    let sql = "SELECT * FROM users ORDER BY ALL";
    let result = Parser::new(&dialect)
        .try_with_sql(sql)
        .unwrap()
        .parse_statements();

    assert!(result.is_ok(), "Custom dialect should parse SQL correctly");
}

/// Example of a custom dialect that inherits from a specific base dialect
/// but adds its own identity
#[derive(Debug)]
struct MyPostgresDialect {
    inner: sqlparser::dialect::PostgreSqlDialect,
}

impl MyPostgresDialect {
    fn new() -> Self {
        Self {
            inner: sqlparser::dialect::PostgreSqlDialect {},
        }
    }
}

impl Dialect for MyPostgresDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        self.inner.is_identifier_start(ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.inner.is_identifier_part(ch)
    }

    // Inherit all PostgreSQL features by delegating
    fn supports_filter_during_aggregation(&self) -> bool {
        self.inner.supports_filter_during_aggregation()
    }

    fn supports_unicode_string_literal(&self) -> bool {
        self.inner.supports_unicode_string_literal()
    }

    // Can override specific methods here if desired
    fn supports_nested_comments(&self) -> bool {
        true // Override to enable nested comments
    }
}

#[test]
fn test_custom_postgres_dialect() {
    let my_postgres = MyPostgresDialect::new();
    let dialect = DialectOverrides::new(my_postgres)
        .supports_order_by_all(true) // Add a feature PostgreSQL doesn't have by default
        .supports_triple_quoted_string(true);

    struct ParserLike<'a> {
        dialect: &'a dyn Dialect,
    }

    macro_rules! dialect_of {
        ( $parsed_dialect: ident is $($dialect_type: ty)|+ ) => {
            ($($parsed_dialect.dialect.is::<$dialect_type>())||+)
        };
    }

    let parser = ParserLike { dialect: &dialect };

    // This will see MyPostgresDialect, not PostgreSqlDialect
    assert!(dialect_of!(parser is MyPostgresDialect));
    assert!(!dialect_of!(parser is sqlparser::dialect::PostgreSqlDialect));

    // But we still have PostgreSQL features
    assert!(Dialect::supports_filter_during_aggregation(&dialect));

    // Plus our custom additions
    assert!(Dialect::supports_order_by_all(&dialect));
    assert!(Dialect::supports_nested_comments(&dialect));
}

/// Example showing a minimalist custom dialect
#[derive(Debug)]
struct MinimalCustomDialect;

impl Dialect for MinimalCustomDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphanumeric() || ch == '_'
    }

    // All other methods use default implementations
}

#[test]
fn test_minimal_custom_dialect() {
    let minimal = MinimalCustomDialect;
    let dialect = DialectOverrides::new(minimal)
        .supports_order_by_all(true)
        .supports_filter_during_aggregation(true)
        .supports_group_by_expr(true);

    struct ParserLike<'a> {
        dialect: &'a dyn Dialect,
    }

    macro_rules! dialect_of {
        ( $parsed_dialect: ident is $($dialect_type: ty)|+ ) => {
            ($($parsed_dialect.dialect.is::<$dialect_type>())||+)
        };
    }

    let parser = ParserLike { dialect: &dialect };

    assert!(dialect_of!(parser is MinimalCustomDialect));
    assert!(Dialect::supports_order_by_all(&dialect));
    assert!(Dialect::supports_filter_during_aggregation(&dialect));
}

/// Example showing how to create a dialect that behaves like multiple dialects
/// by checking for different types
#[derive(Debug)]
struct HybridDialect {
    base: GenericDialect,
}

impl HybridDialect {
    fn new() -> Self {
        Self {
            base: GenericDialect {},
        }
    }
}

impl Dialect for HybridDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        self.base.is_identifier_start(ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.base.is_identifier_part(ch)
    }

    // This hybrid dialect combines features from multiple dialects
    fn supports_filter_during_aggregation(&self) -> bool {
        true // Like PostgreSQL
    }

    fn supports_string_literal_backslash_escape(&self) -> bool {
        true // Like MySQL
    }

    fn supports_nested_comments(&self) -> bool {
        true // Like many dialects
    }
}

#[test]
fn test_hybrid_dialect() {
    let hybrid = HybridDialect::new();
    let dialect = DialectOverrides::new(hybrid)
        .supports_order_by_all(true)
        .supports_triple_quoted_string(true);

    // The dialect has features from multiple SQL dialects
    assert!(Dialect::supports_filter_during_aggregation(&dialect)); // PostgreSQL-like
    assert!(Dialect::supports_string_literal_backslash_escape(&dialect)); // MySQL-like
    assert!(Dialect::supports_nested_comments(&dialect));

    // Plus our custom overrides
    assert!(Dialect::supports_order_by_all(&dialect));
    assert!(Dialect::supports_triple_quoted_string(&dialect));
}
