parser grammar FTParser;

options {
    tokenVocab = FTLexer;
}

root : cmd EOF;

cmd
  : ft_create
  | ft_search
  ;

ft_create : FT DOT CREATE index;

ft_search :  FT DOT SEARCH index;

index : Identifier;
