package com.twitter.scalding.examples

import com.twitter.scalding._

class ParquetCat(args : Args) extends Job(args) {
  Parquet(args("input"), args.list("fields"))
    .read
    .write( Tsv( args("output") ) )
}
