# FromCsv

[![NPM version](https://badge.fury.io/js/fromcsv.svg)](http://badge.fury.io/js/fromcsv)
[![Build Status](https://travis-ci.org/One-com/fromcsv.svg?branch=master)](https://travis-ci.org/One-com/fromcsv)

This module implements an engine for CSV importing.

Compared to other such importers, this lirary is intended to be
far more versataile particularly when it comes to situations
encountered by users with the matching of CSV data using a hash.

## Common user cases

### Missing values

Rows with empty data are competely excluded from the upload. If there
are no values within a columns that would otherwise be unmatched then
that column is ignored.

### Missing columns

Missing columns are not considered a reason to abort processing the file.

### Reordered columns

Columns can be uploaded in any order.

### Unmatched columns

Columns without a match are recorded and be default a file cannot be
uploaded with partial matches. Instead a data structure is returned
which the caller must validate.

## Usage

The engine is ready for use by creating an instance:

```js
const FromCsv = require("fromcsv");

const importer = new FromCsv({
  dialects: {
    some_format: {
      columpMap: {
        "First Column": null,
        "Second Column": null
      }
    }
  }
});
```

This creates an instance of the importer that can be used to match
CSV files of the "some_format" of CSV files. The same instance can
be reused for each attempt to process files with the same columns.

### fromStream

A text stream containing CSV data can be processed by passing it
directly to the `fromStream` method:

```js
const fs = require("fs");

const inputStream = fs.createReadStream("/path/to/file.csv");

importer.fromStream(inputStream, (err, output) => {
  // ...
});
```

### fromData

To submit an unsuccessful payload to the server to be processed
again, the `fromData` method is used:

```js
const data = {
  header: ["Title", "Surname"],
  rows: [["Mr.", "Smith"], ["Mrs.", "Smith"]]
};

importer.fromData(data, (err, output) => {
  // ...
});
```
