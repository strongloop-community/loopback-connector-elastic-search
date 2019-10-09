function buildOrder(model, idName, order) {
  const sort = [];

  let keys = order;
  if (typeof keys === 'string') {
    keys = keys.split(',');
  }
  for (let index = 0, len = keys.length; index < len; index += 1) {
    const m = keys[index].match(/\s+(A|DE)SC$/);
    let key = keys[index];
    key = key.replace(/\s+(A|DE)SC$/, '').trim();
    if (key === 'id' || key === idName) {
      key = '_id';
    }
    if (m && m[1] === 'DE') {
      // sort[key] = -1;
      const temp = {};
      temp[key] = 'desc';
      sort.push(temp);
    } else {
      // sort[key] = 1;
      sort.push(key);
    }
  }

  return sort;
}

module.exports.buildOrder = buildOrder;
