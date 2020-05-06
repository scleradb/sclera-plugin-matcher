# Sclera - Regular Expression Matcher

[![Build Status](https://travis-ci.org/scleradb/sclera-plugin-matcher.svg?branch=master)](https://travis-ci.org/scleradb/sclera-plugin-matcher)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.scleradb/sclera-plugin-matcher_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.scleradb/sclera-plugin-matcher_2.13)
[![scaladoc](https://javadoc.io/badge2/com.scleradb/sclera-plugin-matcher_2.13/scaladoc.svg)](https://javadoc.io/doc/com.scleradb/sclera-plugin-matcher_2.13)

Enables Sclera to efficiently and flexibly analyze ordered streaming data.

The component introduces a construct that enables matching regular expressions over streaming data, and using them to compute sophisticated aggregates. This is a powerful construct, proprietary to Sclera, and enables computations that are ridiculously hard to express and expensive to compute using standard SQL.

For details and examples on using these constructs in a SQL query, please refer to the [ScleraSQL Reference](https://www.scleradb.com/docs/sclerasql/sqlextordered/) document.
