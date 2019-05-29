const BufferedStream = require("bufferedstream");
const sinon = require("sinon");
const stream = require("stream");
const unexpected = require("unexpected");

const FromCsv = require("../lib/FromCsv");

function createDataStream(data) {
  if (Array.isArray(data)) {
    data = data.join("\n");
  }
  return new BufferedStream(data);
}

function importData(fromCsv, data) {
  const dataStream = createDataStream(data);

  return new Promise((resolve, reject) => {
    fromCsv.fromStream(dataStream, (err, output) => {
      if (err) {
        reject(err);
      } else {
        resolve(output);
      }
    });
  });
}

describe("FromCsv", () => {
  it("should throw if missing options", () => {
    unexpected(
      () => {
        // eslint-disable-next-line no-new
        new FromCsv();
      },
      "to throw",
      "FromCsv: missing configuration options"
    );
  });

  it("should throw if options contains no dialects", () => {
    unexpected(
      () => {
        // eslint-disable-next-line no-new
        new FromCsv({});
      },
      "to throw",
      "FromCsv: no dialects specified"
    );
  });

  it("should throw if processRowCoalesce was not a function", () => {
    unexpected(
      () => {
        // eslint-disable-next-line no-new
        new FromCsv({
          dialects: {
            test_standard: {
              columnMap: {
                Foo: null,
                Bar: null
              }
            }
          },
          processRowCoalesce: []
        });
      },
      "to throw",
      "FromCsv: invalid processRowCoalesce supplied"
    );
  });
});

describe("fromStream", () => {
  let fromCsv;
  const expect = unexpected
    .clone()
    .use(require("unexpected-sinon"))
    .addAssertion(
      "<array|string> when imported to return output satisfying <object>",
      (expect, subject, value) => {
        return expect(importData(fromCsv, subject), "to be fulfilled").then(
          output => {
            expect(output, "to satisfy", value);
          }
        );
      }
    );

  it("should handle input stream errors gracefully", function() {
    var erroringStream = stream.Readable();
    erroringStream._read = function() {
      this.emit("error", Error("fake error"));
    };

    return unexpected(cb => {
      new FromCsv({
        dialects: {
          test_standard: {
            columnMap: {
              Foo: null,
              Bar: null
            }
          }
        }
      }).fromStream(erroringStream, cb);
    }, "to call the callback").spread(function(err, output) {
      unexpected(err, "to be defined");
      unexpected(err.message, "to be", "fake error");
      unexpected(output, "to be undefined");
    });
  });

  describe("when configured with a columnMap", () => {
    beforeEach(() => {
      fromCsv = new FromCsv({
        dialects: {
          test_standard: {
            columnMap: {
              "First Name": null,
              "Other Name": "Middle Name",
              "Last Name": null
            }
          }
        }
      });
    });

    it("should ignore empty file", () => {
      const data = ["", "", "", "", ""];

      return expect(data, "when imported to return output satisfying", {
        isComplete: true,
        rowObjects: []
      });
    });

    it("should ignore empty rows", () => {
      const data = [",,", ",", ",,", ",", ",,,,,,"];

      return expect(data, "when imported to return output satisfying", {
        isComplete: true,
        rowObjects: []
      });
    });

    it("should ignore no rows", () => {
      const data = [",,"];

      return expect(data, "when imported to return output satisfying", {
        isComplete: true,
        rowObjects: []
      });
    });

    it("should pass through empty header row", () => {
      const data = ["", "", "a,a", "b,b"];

      return expect(data, "when imported to return output satisfying", {
        isComplete: false,
        columns: {
          type: "test_standard",
          present: [null, null],
          unmatched: [null, null]
        },
        rows: [["a", "a"], ["b", "b"]]
      });
    });

    it("should pass through row with more values than columns (invalid csv)", () => {
      const data = ["First Name", "john,", ",doe"];

      return expect(data, "when imported to return output satisfying", {
        isComplete: false,
        columns: {
          type: "test_standard",
          present: ["First Name", null],
          unmatched: [null]
        },
        rows: [["john", ""], ["", "doe"]]
      });
    });

    it("should pass through rows with an unmtached header where the second has one less value", () => {
      const data = ["First Name,unidentified", "john,doe", "foo,"];

      return expect(data, "when imported to return output satisfying", {
        isComplete: false,
        columns: {
          type: "test_standard",
          present: ["First Name", null],
          unmatched: ["unidentified"]
        },
        rows: [["john", "doe"], ["foo", null]]
      });
    });

    it("should import row with more values than columns", () => {
      const data = ["First Name", "john,"];

      return expect(data, "when imported to return output satisfying", {
        isComplete: true,
        rowObjects: [{ "First Name": "john" }]
      });
    });

    it("should import row with less values than columns", () => {
      const data = ["First Name,Last Name,", "john,", ",doe"];

      return expect(data, "when imported to return output satisfying", {
        isComplete: true,
        rowObjects: [{ "First Name": "john" }, { "Last Name": "doe" }]
      });
    });

    it("should import rows with unequal number of values", () => {
      const data = ["First Name,Last Name,", "john,", ",doe", ",smith,"];

      return expect(data, "when imported to return output satisfying", {
        isComplete: true,
        rowObjects: [
          { "First Name": "john" },
          { "Last Name": "doe" },
          { "Last Name": "smith" }
        ]
      });
    });
  });

  describe("when configured with a languageMap", () => {
    beforeEach(() => {
      fromCsv = new FromCsv({
        dialects: {
          test_languageMap: {
            columnMap: {
              "First Name": null,
              "Other Name": "Middle Name",
              "Last Name": null
            },
            languageMap: {
              "First Name": ["First-o Name-o"],
              "Other Name": ["Middle Name"],
              "Last Name": ["Family Name"]
            }
          }
        }
      });
    });

    it("should import rows with unequal number of values", () => {
      const data = ["First-o Name-o,Family Name,", "john,", ",doe", ",smith,"];

      return expect(data, "when imported to return output satisfying", {
        isComplete: true,
        rowObjects: [
          { "First Name": "john" },
          { "Last Name": "doe" },
          { "Last Name": "smith" }
        ]
      });
    });

    it("should allow ambiguous column definitions", () => {
      fromCsv = new FromCsv({
        dialects: {
          test_standard: {
            columnMap: {
              Anything: "anything"
            },
            languageMap: {
              Anything: ["First-o Name-o", "Last-o Name-o"]
            }
          }
        }
      });

      const data = [["Anything", "Anything"], ["john", "smith"]];

      return expect(data, "when imported to return output satisfying", {
        isComplete: true,
        rowObjects: [{ anything: ["john", "smith"] }]
      });
    });

    it("should call row coalesce with aliases object containg the original columns", () => {
      const processRowCoalesce = sinon
        .stub()
        .named("processRowCoalese")
        .returnsArg(0);
      fromCsv = new FromCsv({
        dialects: {
          test_languageMap: {
            columnMap: {
              "First Name": null,
              "Other Name": "Middle Name",
              "Last Name": null
            },
            languageMap: {
              "First Name": ["First-o Name-o"],
              "Other Name": ["Middle Name"],
              "Last Name": ["Family Name"]
            }
          }
        },
        processRowCoalesce
      });
      const data = ["First-o Name-o,Family Name,", "john,", ",doe", ",smith,"];

      return expect(data, "when imported to return output satisfying", {
        isComplete: true
      }).then(() => {
        expect(processRowCoalesce, "to have calls satisfying", [
          [{}, { "First Name": "First-o Name-o" }, {}, {}],
          [{}, { "Last Name": "Family Name" }, {}, {}],
          [{}, { "Last Name": "Family Name" }, {}, {}]
        ]);
      });
    });
  });

  describe("when configured with multiple dialects", () => {
    beforeEach(() => {
      fromCsv = new FromCsv({
        dialects: {
          test_dialect_1: {
            columnMap: {
              Name: null,
              Foo: null,
              Baz: null
            }
          },
          test_dialect_2: {
            columnMap: {
              Name: null,
              Foo: null,
              Bar: null
            }
          }
        }
      });
    });

    it("should select the second dialect", () => {
      const data = ["Name,Foo,Bar", "Tester,foo,bar"];

      return expect(data, "when imported to return output satisfying", {
        isComplete: true,
        rowObjects: [{ Name: "Tester", Foo: "foo", Bar: "bar" }]
      });
    });

    it("should select the first dialect", () => {
      const data = ["Name,Foo,Baz", "Tester,foo,baz"];

      return expect(data, "when imported to return output satisfying", {
        isComplete: true,
        rowObjects: [{ Name: "Tester", Foo: "foo", Baz: "baz" }]
      });
    });
  });
});

describe("fromData", function() {
  const fromCsv = new FromCsv({
    dialects: {
      test_format: {
        columnMap: {
          Foo: null,
          Bar: null
        }
      }
    }
  });

  it("should error when the header is missing", function() {
    unexpected(
      () => {
        fromCsv.fromData(null);
      },
      "to throw",
      new Error("Missing input data.")
    );
  });

  it("should error when the header is missing", function() {
    unexpected(
      () => {
        fromCsv.fromData({});
      },
      "to throw",
      new Error("Invalid header.")
    );
  });

  it("returns row objects", function() {
    const data = {
      header: ["Foo", "Bar"],
      rows: [["Mr.", "Smith"], ["Mrs.", "Smith"]]
    };

    unexpected(fromCsv.fromData(data), "to satisfy", {
      isComplete: true,
      rowObjects: [
        {
          Foo: "Mr.",
          Bar: "Smith"
        },
        {
          Foo: "Mrs.",
          Bar: "Smith"
        }
      ]
    });
  });

  it("returns row objects while handling empty values", function() {
    const data = {
      header: ["Foo", null],
      rows: [["Mr.", "Smith"]]
    };

    unexpected(fromCsv.fromData(data), "to satisfy", {
      isComplete: false,
      rows: [["Mr.", "Smith"]]
    });
  });

  it("offers simple data sanitization based on validity of rows", function() {
    const data = {
      header: ["Foo"],
      rows: [["Smith"], ["", ""]]
    };

    unexpected(fromCsv.fromData(data), "to satisfy", {
      isComplete: true,
      rowObjects: [
        {
          Foo: "Smith"
        }
      ]
    });
  });

  it("offers simple data sanitization based on field count", function() {
    const data = {
      header: ["Bar"],
      rows: [["Smith"], ["Doe", ""]]
    };

    unexpected(fromCsv.fromData(data), "to satisfy", {
      isComplete: true,
      rowObjects: [
        {
          Bar: "Smith"
        },
        {
          Bar: "Doe"
        }
      ]
    });
  });

  it("sanitizes CRLF EOL characters to LF", function() {
    const data = {
      header: ["Foo", "Bar"],
      rows: [["Smith", "this is\r\na note"]]
    };

    unexpected(fromCsv.fromData(data), "to satisfy", {
      isComplete: true,
      rowObjects: [
        {
          Foo: "Smith",
          Bar: "this is\na note"
        }
      ]
    });
  });

  it("should pass through unknown data (row validity)", function() {
    const data = {
      header: ["Last Name"],
      rows: [["Smith"], ["", ""]]
    };

    unexpected(fromCsv.fromData(data), "to satisfy", {
      isComplete: false,
      columns: {
        type: "test_format",
        present: [null],
        missing: [],
        unmatched: ["Last Name"]
      },
      rows: [["Smith"]]
    });
  });

  it("should pass through unknown data (field count)", function() {
    const data = {
      header: ["Last Name"],
      rows: [["Smith"], ["Doe", ""]]
    };

    unexpected(fromCsv.fromData(data), "to satisfy", {
      isComplete: false,
      columns: {
        type: "test_format",
        present: [null],
        missing: [],
        unmatched: ["Last Name"]
      },
      rows: [["Smith"], ["Doe"]]
    });
  });

  describe("with forced import", () => {
    const forceImport = true;

    it("should store unknown data (row validity)", function() {
      const data = {
        header: ["Last Name"],
        rows: [["Smith"], ["", ""]]
      };

      unexpected(fromCsv.fromData(data, forceImport), "to satisfy", {
        isComplete: true,
        rowObjects: [
          {
            unknowns: {
              "Last Name": "Smith"
            }
          }
        ]
      });
    });

    it("should store unknown data (field count)", function() {
      const data = {
        header: ["Last Name"],
        rows: [["Smith"], ["Doe", ""]]
      };

      unexpected(fromCsv.fromData(data, forceImport), "to satisfy", {
        isComplete: true,
        rowObjects: [
          {
            unknowns: {
              "Last Name": "Smith"
            }
          },
          {
            unknowns: {
              "Last Name": "Doe"
            }
          }
        ]
      });
    });

    it("should ignore explcitly marked unknown data", function() {
      const data = {
        header: ["Foo", null],
        rows: [["Doe", "something"]]
      };

      unexpected(
        fromCsv.fromData(data, forceImport),
        "to exhaustively satisfy",
        {
          isComplete: true,
          rowObjects: [
            {
              Foo: "Doe",
              unknowns: {
                1: "something"
              }
            }
          ]
        }
      );
    });

    it("returns row objects while handling unknown header", function() {
      const data = {
        header: ["Foo", "xxx"],
        rows: [["Mr.", "Smith"]]
      };

      unexpected(fromCsv.fromData(data, true), "to satisfy", {
        isComplete: true,
        rowObjects: [{ Foo: "Mr.", unknowns: { xxx: "Smith" } }]
      });
    });

    it("returns row objects while handling empty header", function() {
      const data = {
        header: ["Foo", null],
        rows: [["Mr.", "Smith"]]
      };

      unexpected(fromCsv.fromData(data, true), "to satisfy", {
        isComplete: true,
        rowObjects: [{ Foo: "Mr.", unknowns: { 1: "Smith" } }]
      });
    });
  });
});
