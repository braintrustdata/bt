const { greet } = require("./utils.cjs");

module.exports.greetAll = function (names) {
  return names.map(greet);
};
