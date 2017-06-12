const es          = require('elasticsearch')
const promise     = require('bluebird')
const _           = require('lodash')
const Boom        = require('boom')
const bodyBuilder = require('bodybuilder')
const chalk       = require('chalk')


module.exports = (options) => {

  const defaults = {
    size: 10,
    query() {
      const body = bodyBuilder()
        .query('match_all')
      return body.build()
    }
  }

  const privates = {
    validateOptions (options) {
      return _.has(options, 'couchbase.host', 'couchbase.bucket', 'elasticsearch.host', 'elasticsearch.port', 'elasticsearch.index')
    },
    search (type, query) {
      const client = new es.Client({
        host: `${options.elasticsearch.host}:${options.elasticsearch.port}`
      })
      query.index = options.elasticsearch.index
      query.type  = type
      return client.search(query)
    },
    migrate (docs) {
      let promises = []
      _.each(docs, (doc) => {
        promises.push(
          db.replace(doc._id, doc)
        )
      })
      return promise.all(promises)
    },
    getChalk (status) {
      switch (status) {
        case "noNeed": return chalk.blue
        case "successful": return chalk.green
        case "unsuccessful": return chalk.red
        case "notFound": return chalk.magenta
      }
    },
    db: null,
    addPropertyMigrator (keys, db, propertyName, defaultValue) {
      const promises = []
      _.each(keys, (key) => {
        promises.push(
          db.get(key).then( (result) => {
            if(result instanceof Error)
              return wm.addStat(key, wm.statuses.notFound)
            let doc = result.value
            if(doc[propertyName] != null && doc[propertyName] != undefined) {
              return wm.addStat(key, wm.statuses.noNeed)
            }
            const updateObject = {}
            updateObject[propertyName] = defaultValue
            return db.update(key, updateObject)
              .then( (result) => {
                if(result instanceof Error)
                  return wm.addStat(key, wm.statuses.unsuccessful)
                return wm.addStat(key, wm.statuses.successful)
              })
          })
        )
      })
      return promise.all(promises)
    }
  }

  const wm = {
    update (type, query, migrator, page = 0) {
      const isValid = privates.validateOptions(options)
      if(!isValid) return promise.resolve(new Error('options is not valid'))
      if(privates.db == null)
        privates.db = new require('puffer') ({ host: options.couchbase.host, name: options.couchbase.bucket })
      body = defaults.query()
      if(query != null) body.query = query
      body.from = (page * defaults.size)
      body.size = defaults.size
      return privates.search(type, { body })
        .then( (results) => {
          if(results instanceof Error) return Error
          if(results.hits.hits.length == 0) {
            _.each(wm.results, (result, key) => {
              console.log(privates.getChalk(result)(`${key}: ${result}`))
            })
            console.log(wm.stats)
            privates.db.bucket.disconnect()
            privates.db = null
            return true
          }
          const keys = _.map(results.hits.hits, '_id')
          return migrator(keys, privates.db)
            .then( () => {
              return wm.update(type, query, migrator, page + 1)
            })
        })
    },
    statuses: {
      noNeed: "noNeed",
      successful: "successful",
      unsuccessful: "unsuccessful",
      notFound: "notFound"
    },
    stats: {
      noNeed: 0,
      successful: 0,
      unsuccessful: 0,
      notFound: 0
    },
    results: {},
    addStat (key, status) {
      wm.stats[status]++
      wm.results[key] = status
      return true
    },
    addProperty (type, query, propertyName, defaultValue) {
      return wm.update(type, query, (keys, db) => {
        return privates.addPropertyMigrator(keys, db, propertyName, defaultValue)
      })
    }
  }

  return wm
}