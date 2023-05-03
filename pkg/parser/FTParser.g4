parser grammar FTParser;

options {
    tokenVocab = FTLexer;
}

root : cmd EOF;

cmd
  : ft_create
  | ft_search
  ;

index : Identifier;
field_name : Identifier;

ft_create : FT DOT CREATE index prefix_part? SCHEMA field_spec+;

prefix_part : PREFIX Integral prefix+; // TODO refine prefix value parsing
prefix : Identifier+;

field_spec : field_name field_type;

field_type : TEXT | TAG | NUMERIC | GEO | VECTOR;

ft_search : FT DOT SEARCH index query limit_part?;

query : query_part;

query_part
  : non_union_query_part+
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

word : Identifier | Integral;

field_ref : FieldIdentifier COLON;

limit_part : LIMIT offset num;
offset : Integral;
num : Integral;
