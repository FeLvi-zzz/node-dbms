import { Parser } from 'node-sql-parser'
import fs from 'fs'
import { ulid } from 'ulid'

if (!fs.existsSync("db")) {
  fs.mkdirSync("db")
}

const schemaIO = {
  read: () => {
    if (!fs.existsSync("db/.schema.json")) {
      fs.writeFileSync("db/.schema.json", "{}")
    }
    const file = fs.readFileSync("db/.schema.json", { encoding: "utf-8" })
    return JSON.parse(file)
  },
  write: (obj: Object) => {
    fs.writeFileSync("db/.schema.json", JSON.stringify(obj))
  }
}

const tableIO = (tableName: string) => ({
  read: () => {
    const file = fs.readFileSync(`db/${tableName}.json`, { encoding: "utf-8" })
    return JSON.parse(file)
  },
  write: (obj: Object) => {
    fs.writeFileSync(`db/${tableName}.json`, JSON.stringify(obj))
  },
  delete: () => {
    fs.unlinkSync(`db/${tableName}.json`)
  }
})

const whereFilter = (table: any[], where: any) => {
  if (where.operator === "AND" || where.operator === "&&") {
    const leftSet = new Set(whereFilter(table, where.left).map((record: any) => record.ulid))
    const rightSet = new Set(whereFilter(table, where.right).map((record: any) => record.ulid))
    const andSet = new Set()
    for (let elem of Array.from(leftSet)) {
      if (rightSet.has(elem)) {
        andSet.add(elem)
      }
    }
    return table.filter((record: any) => andSet.has(record.ulid))
  } else if (where.operator === "OR" || where.operator === "||") {
    const leftTable: any[] = whereFilter(table, where.left)
    const rightTable: any[] = whereFilter(table, where.right)
    const orSet = new Set([...leftTable.map(record => record.ulid), ...rightTable.map(record => record.ulid)])
    return table.filter((record: any) => orSet.has(record.ulid))
  } else if(where.operator === "=") {
    return table.filter((record: any) => record.value[where.left.column] === where.right.value)
  } else if (where.operator === "!=") {
    return table.filter((record: any) => record.value[where.left.column] !== where.right.value)
  } else if (where.operator === "IS") {
    return table.filter((record: any) => record.value[where.left.column] == where.right.value)
  } else if (where.operator === "IS NOT") {
    return table.filter((record: any) => record.value[where.left.column] != where.right.value)
  } else if (where.operator === ">") {
    return table.filter((record: any) => record.value[where.left.column] > where.right.value)
  } else if (where.operator === "<") {
    return table.filter((record: any) => record.value[where.left.column] < where.right.value)
  }
  return []
}

const parser = new Parser()
const ast = parser.astify(process.argv[2]) as any

if (ast.keyword === "table") {
  const schema = schemaIO.read()

  if (ast.type === "create") {
    const tableName = ast.table[0].table
    tableIO(tableName).write([])

    schema[tableName] = {}

    ast.create_definitions.forEach((createDifinition: any) => {
      schema[tableName][createDifinition.column.column] = createDifinition.definition.dataType
    })
  } else if (ast.type === "drop") {
    const tableName = ast.name[0].table
    tableIO(tableName).delete()
    delete schema[tableName]
  }

  schemaIO.write(schema)
} else if (ast.type === "insert") {
  const tableName = ast.table[0].table
  const table = tableIO(tableName).read()

  ast.values.forEach((value: any) => {
    const newValue = {} as any
    for (let i=0; i < ast.columns.length; i++) {
      newValue[ast.columns[i]] = value.value[i].value
    }
    table.push({
      ulid: ulid(),
      value: newValue
    })
  })

  tableIO(tableName).write(table)
} else if (ast.type === "select") {
  const tableName = ast.from[0].table
  const table = tableIO(tableName).read()
  if (ast.columns === "*") {
    console.log(table.map((value: any) => value.value))
  } else {
    let newTable = table

    if (ast.where) {
      newTable = whereFilter(newTable, ast.where)
    }

    if (ast.orderby) {
      newTable.sort((a: any, b: any) => {
        for (let i=0; i < ast.orderby.length; i++) {
          const orderby = ast.orderby[i]
          const columnName = orderby.expr.column
          const orderFlag = orderby.type === "ASC" ? -1 : 1
          if (a.value[columnName] < b.value[columnName]) {
            return orderFlag
          } else if (a.value[columnName] > b.value[columnName]) {
            return -orderFlag
          }
        }
        return 0
      })
    }

    newTable = newTable.map((value: any) => {
      const newValue = {} as any
      ast.columns.forEach((column: any) => {
        newValue[column.expr.column] = value.value[column.expr.column]
      })
      return newValue
    })

    if (ast.limit) {
      const limit = ast.limit.value[0].value
      const offset = ast.limit.value[1]?.value ?? 0
      newTable = newTable.slice(offset, offset + limit)
    }

    console.log(newTable)
  }
} else if (ast.type === "delete") {
  const tableName = ast.from[0].table
  const table = tableIO(tableName).read()
  tableIO(tableName).write(table.filter(() => false))
} else if (ast.type === "update") {
  const tableName = ast.table[0].table
  const table = tableIO(tableName).read()
  tableIO(tableName).write(table.map((record: any) => {
    const newValue = record.value
    ast.set.forEach((set: any) => {
      newValue[set.column] = set.value.value
    })
    return {
      ...record,
      value: newValue
    }
  }))
}
