parser grammar QueryParser;

options {
  tokenVocab = QueryLexer;
}

query : query_part EOF;

query_part
  : non_union_query_part
  | query_part query_part
  | query_part OR query_part;

non_union_query_part
  : parenthesized_query_part
  | field_query_part
  | simple_query_part;

field_query_part : field_ref non_union_query_part; // TODO: 02/05/2023 improve parsing by forbidding nested field refs.
                                                   //       This can be done by having separate cases for parenthesized non-union and parenthesized field-referenced

simple_query_part
  : word
  | exact_match;

parenthesized_query_part : LBRACE query_part RBRACE;

exact_match : QUOTE word+ QUOTE;

word : String | EscapedString;

field_ref : FieldIdentifier COLON;
