var BufferedStream = require("bufferedstream");
var iconv = require("iconv");

function bufferFromGuess(buffer) {
  var charset;

  for (var i = 0; i < buffer.length; i += 1) {
    if (buffer[i] > 0x7f) {
      // Found an octet that has the 8th bit set. The file cannot be ASCII.

      try {
        // Try decoding it as UTF-8
        return new iconv.Iconv("utf-8", "UTF-8").convert(buffer);
      } catch (e) {
        // Assume EINVAL
        charset = "iso-8859-1";

        for (var j = i; i < buffer.length; i += 1) {
          if (buffer[j] >= 128 && buffer[i] <= 159) {
            charset = "windows-1252";
            break;
          }
        }
      }

      try {
        return new iconv.Iconv(charset, "UTF-8").convert(buffer);
      } catch (e) {
        // our final fallback is to decode as DOS CP850
        return new iconv.Iconv("cp850", "UTF-8").convert(buffer);
      }
    }
  }

  return new iconv.Iconv("us-ascii", "UTF-8").convert(buffer);
}

module.exports = function bufferStreamAndGuessEncoding(stream, callback) {
  var chunks = [];

  stream
    .on("data", function(chunk) {
      if (typeof chunk === "string") {
        chunk = new Buffer(chunk);
      }
      chunks.push(chunk);
    })
    .on("end", function() {
      var buffer = bufferFromGuess(Buffer.concat(chunks));
      var stream = new BufferedStream();

      // write our gussed buffer to the output stream
      stream.end(buffer);

      callback(null, stream);
    })
    .on("error", callback);
};
