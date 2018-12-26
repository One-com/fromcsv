var _ = require("lodash");
var csv = require("ya-csv");

var bufferStreamAndGuessEncoding = require("./bufferStreamAndGuessEncoding");

module.exports = function FromCsv(options) {
  if (!(typeof options === "object" && options !== null)) {
    throw new Error("FromCsv: missing configuration options");
  }

  const dialects = options.dialects;
  if (!(typeof dialects === "object" && dialects !== null)) {
    throw new Error("FromCsv: no dialects specified");
  }

  let processRowCoalesce = options.processRowCoalesce;
  if (processRowCoalesce && typeof processRowCoalesce !== "function") {
    throw new Error("FromCsv: invalid processRowCoalesce supplied");
  }

  function defaultProcessRowCoalesce(obj, aliases, unknowns) {
    if (Object.keys(unknowns).length > 0) {
      const tmp = (obj.unknowns = {});
      Object.keys(unknowns).forEach(unknownKey => {
        const originalUnknownKey = unknowns[unknownKey];
        if (originalUnknownKey) {
          tmp[originalUnknownKey] = obj[unknownKey];
        } else {
          tmp[unknownKey] = obj[unknownKey];
        }
        delete obj[unknownKey];
      });
    }
    return obj;
  }

  // default it if not supplied
  processRowCoalesce = processRowCoalesce || defaultProcessRowCoalesce;

  var configurations = initialiseConfig(options.dialects);

  function configForType(csvType) {
    var csvConfig;

    if (
      !(
        csvType &&
        (csvConfig = configurations.find(
          csvConfig => csvConfig.type === csvType
        )) !== undefined
      )
    ) {
      throw new Error("Invalid CSV dialect requested.");
    }

    return csvConfig;
  }

  function arrayCountNull(arr) {
    return arr.filter(isNull).length;
  }

  function buildHeaderMatchHash(columnMap) {
    var hash = {};

    Object.keys(columnMap).forEach(function(columnName) {
      // use the english name (first element) as the resolved name
      var resolvedName = columnName;

      // include the column name itself in the of allowed names
      var namesList = [columnName].concat(columnMap[columnName]);

      // add each language specific column name to the match hash
      namesList.forEach(function(columnName) {
        const lastResolvedName = hash[columnName];
        if (lastResolvedName && lastResolvedName !== resolvedName) {
          // convert to an array in the case of an overlapping match
          if (!Array.isArray(lastResolvedName)) {
            hash[columnName] = [lastResolvedName];
          }
          // add the new resolved name to the list
          hash[columnName].push(resolvedName);
        } else {
          // record a resolution mapping
          hash[columnName] = resolvedName;
        }
      });
    });

    return hash;
  }

  function buildMatchHash(matchedNames) {
    var hash = {};

    matchedNames.forEach(function(matchName, elemIndex) {
      hash[elemIndex] = matchName;
    });

    return hash;
  }

  function findAllIndexes(array, cmp) {
    var foundIndexes = [];
    var fromIndex = 0;

    while (fromIndex !== -1 && fromIndex < array.length) {
      var foundIndex = _.findIndex(array, cmp, fromIndex);
      if (foundIndex !== -1) {
        foundIndexes.push(foundIndex);
        fromIndex = foundIndex + 1;
      } else {
        fromIndex = -1;
      }
    }

    return foundIndexes;
  }

  function getColumnCountFromRows(csvDataRows) {
    var columnCount = 0;

    // find the greatest row length
    csvDataRows.forEach(
      row => (columnCount = Math.max(row.length, columnCount))
    );

    return columnCount;
  }

  function getDataPresenceFromRows(csvRows) {
    var dataPresent = {};

    csvRows.forEach(row => {
      var foundIndexes = findAllIndexes(row, elem => elem.length > 0);

      // record them seen
      foundIndexes.forEach(index => (dataPresent[index] = true));
    });

    return dataPresent;
  }

  function isBetterHeaderMatch(contender, original) {
    return arrayCountNull(contender) < arrayCountNull(original);
  }

  function isEmptyRow(row) {
    return row.length === 0 || row.every(value => !value);
  }

  function isValidRow(row) {
    return !(row.length === 1 && row[0] === "");
  }

  function isNull(object) {
    return object === null;
  }

  function matchHeaderByHash(matchHash, headerRow) {
    var matchedNames = [];
    var unmatchedNamesByIndex = {};
    var seen = {};

    headerRow.forEach(function(columnName, columnIndex) {
      columnName = columnName || null; // swap empty names for null

      var matches = matchHash[columnName];
      if (matches) {
        let matchedName = Array.isArray(matches) ? matches[0] : matches;
        const seenIndex = seen[matchedName];
        if (seenIndex) {
          // use the seen count as index to the next match
          matchedName = matches[seenIndex];

          // increment the seen count
          seen[matchedName] += 1;
        } else {
          // record that the match was seen
          seen[matchedName] = 1;
        }

        matchedNames.push(matchedName);
      } else {
        matchedNames.push(null);
        unmatchedNamesByIndex[columnIndex] = columnName;
      }
    });

    return {
      matchedNames,
      unmatchedNamesByIndex
    };
  }

  function processRow(csvConfig, headerInfo, dataRow) {
    var columnMap = csvConfig.columnMap;
    var columnNamesByIndex = headerInfo.matchedColumnsByIndex;
    var originalColumns = headerInfo.originalColumns;

    var contact = {};
    var aliases = {};
    var unknowns = {};

    dataRow.forEach(function(value, columnIndex) {
      var columnName;
      // protect against the row containing more data than the columns
      if (columnNamesByIndex[columnIndex] !== undefined) {
        columnName = columnNamesByIndex[columnIndex];
      } else {
        columnName = null;
      }

      if (!value) {
        // skip if there is no value for the field
        return;
      }

      const isUnmapped = columnName === null;
      if (isUnmapped) {
        columnName = columnIndex;
      }

      // normalize line feeds
      value = value.replace(/\r\n/g, "\n");

      // was there a mapping present for the column
      var mapper = columnMap[columnName];
      if (mapper) {
        if (typeof mapper === "function") {
          // execute it as a function
          mapper(contact, value);
        } else if (contact[mapper]) {
          const existing = contact[mapper];
          if (!Array.isArray(existing)) {
            contact[mapper] = [existing];
          }
          contact[mapper].push(value);
        } else {
          contact[mapper] = value;
        }
      } else if (!isUnmapped) {
        contact[columnName] = value;
        aliases[columnName] = originalColumns[columnIndex];
      } else {
        contact[columnName] = value;
        unknowns[columnName] = originalColumns[columnIndex];
      }
    });

    return processRowCoalesce(contact, aliases, unknowns);
  }

  function initialiseConfig(inputConfig) {
    return Object.keys(inputConfig).map(function(csvType) {
      return initialiseCsvConfig(inputConfig[csvType], csvType);
    });
  }

  function initialiseCsvConfig(typeConfig, csvType) {
    var csvConfig = {
      type: csvType,
      columnMap: typeConfig.columnMap,
      hasFixedColumns: false,
      headerMatchHash: null
    };

    if (typeConfig.languageMap) {
      csvConfig.headerMatchHash = buildHeaderMatchHash(typeConfig.languageMap);
    } else {
      const headerMatchHash = {};
      const columns = Object.keys(typeConfig.columnMap);
      columns.forEach(function(columnName) {
        headerMatchHash[columnName] = columnName;
      });

      csvConfig.hasFixedColumns = true;
      csvConfig.headerMatchHash = headerMatchHash;
    }

    return csvConfig;
  }

  function attemptMatchHeader(headerRow) {
    var matchedNames = null;
    var unmatchedNamesByIndex = null;
    var matchedType = null;

    configurations.forEach(function(csvConfig) {
      var nextHeaderMatch = matchHeaderByHash(
        csvConfig.headerMatchHash,
        headerRow
      );

      // did we match column names?
      if (
        matchedNames === null ||
        isBetterHeaderMatch(nextHeaderMatch.matchedNames, matchedNames)
      ) {
        // set the match state
        matchedType = csvConfig.type;
        matchedNames = nextHeaderMatch.matchedNames;
        unmatchedNamesByIndex = nextHeaderMatch.unmatchedNamesByIndex;
      }
    });

    return {
      type: matchedType,
      matchedColumns: matchedNames,
      matchedColumnsByIndex: buildMatchHash(matchedNames),
      originalColumns: headerRow,
      unmatchedColumnsByIndex: unmatchedNamesByIndex
    };
  }

  function processCsvData(headerInfo, csvRows, forceImport = false) {
    var csvConfig = configForType(headerInfo.type);
    var csvDataPresent = {};

    var hasRows = csvRows.length > 0;
    if (hasRows) {
      _.remove(csvRows, isEmptyRow);

      // fill in column names based on row length if required
      var columnCount = headerInfo.matchedColumns.length;
      var extraColumnCount = getColumnCountFromRows(csvRows) - columnCount;
      while (extraColumnCount > 0) {
        headerInfo.matchedColumns.push(null);
        headerInfo.unmatchedColumnsByIndex[
          columnCount + extraColumnCount - 1
        ] = null;
        extraColumnCount -= 1;
      }

      // immediately check the rows so we know what data they contained
      csvDataPresent = getDataPresenceFromRows(csvRows);

      // update the unmatched columns index based on the contained data
      Object.keys(headerInfo.unmatchedColumnsByIndex).forEach(function(
        columnIndex
      ) {
        if (!(columnIndex in csvDataPresent)) {
          delete headerInfo.unmatchedColumnsByIndex[columnIndex];
        }
      });
    }

    var isComplete = forceImport
      ? true
      : Object.keys(headerInfo.unmatchedColumnsByIndex).length === 0;

    var rowObjects;
    if (isComplete) {
      if (hasRows && Object.keys(csvDataPresent).length > 0) {
        rowObjects = csvRows.map(dataRow =>
          processRow(csvConfig, headerInfo, dataRow)
        );
      } else {
        rowObjects = [];
      }

      return {
        isComplete: true,
        rowObjects
      };
    }

    // restrict required column names by those which contained data
    var columns = hasRows
      ? headerInfo.matchedColumns.filter(
          (elem, index) => index in csvDataPresent
        )
      : headerInfo.matchedColumns;
    // pair down the rows based on their contained data
    var rows = csvRows.map(function(csvRow) {
      const row = csvRow.filter((elem, index) => index in csvDataPresent);
      // fill in gaps if the row is shorter than the number of matched columns
      let extraColumnCount = columns.length - row.length;
      while (extraColumnCount > 0) {
        row.push(null);
        extraColumnCount -= 1;
      }
      return row;
    });

    return {
      isComplete: false,
      columns: {
        type: headerInfo.type,
        present: columns,
        missing: _.difference(headerInfo.matchedColumns, columns),
        unmatched: Object.keys(headerInfo.unmatchedColumnsByIndex).map(
          key => headerInfo.unmatchedColumnsByIndex[key]
        )
      },
      rows: rows
    };
  }

  function processUtf8Stream(data, callback) {
    var csvReader = new csv.CsvReader(data);
    var csvRows = [];
    var headerInfo = null;
    var seenFirstRow = false;

    csvReader.on("data", function(row) {
      if (!seenFirstRow) {
        // process the first row
        headerInfo = attemptMatchHeader(row);

        // mark we saw the first row
        seenFirstRow = true;
      } else {
        // normal row stuff
        csvRows.push(isValidRow(row) ? row : []);
      }
    });

    csvReader.on("error", callback);

    csvReader.on("end", function() {
      try {
        var result = processCsvData(headerInfo, csvRows);
        callback(null, result);
      } catch (e) {
        callback(e);
      }
    });
  }

  this.fromStream = function fromStream(inputStream, callback) {
    bufferStreamAndGuessEncoding(inputStream, function(err, stream) {
      if (err) {
        return callback(err);
      }

      // ensure data is emitted as a string as is required by ya-csv
      stream.setEncoding("utf8");

      processUtf8Stream(stream, callback);
    });
  };

  this.fromData = function fromData(inputData, forceImport = false) {
    if (typeof inputData !== "object" || inputData === null) {
      throw new Error("Missing input data.");
    }

    if (!inputData.header) {
      throw new Error("Invalid header.");
    }

    var headerInfo = attemptMatchHeader(inputData.header);

    return processCsvData(headerInfo, inputData.rows, forceImport);
  };
};
