const { greet } = require("./utils");

module.exports.greetAll = function(names) {
  return names.map(greet);
}
