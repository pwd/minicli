var findById = function(col, _id) {
    return db[col].findOne({_id: _id});
};
