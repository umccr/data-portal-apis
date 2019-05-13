import * as squel from "squel";

const COLUMN_TYPE_STRING = 'string';
const COLUMN_TYPE_INTEGER = 'integer';
const COLUMN_TYPE_DATETIME = 'datetime';

const FILTER_COLUMNS = {
    key: {
        type: COLUMN_TYPE_STRING
    },
    size: {
        type: COLUMN_TYPE_INTEGER
    },
    last_modified_date: {
        type: COLUMN_TYPE_DATETIME
    }
};

const FILTERS = {
    default: 'key.include',
    filters: [
        {
            column: 'key',
            type: 'end_with',
            alias: 'ext',
            description: 'File extension'
        },
        {
            column: 'key',
            type: 'include',
            alias: 'pathinc',
            description: 'File path includes'
        },
        {
            column: 'size',
            type: 'compare',
            alias: 'size',
            description: 'Compare with file size'
        },
        {
            column: 'last_modified_date',
            type: 'compare',
            alias: 'date',
            description: 'Compare with last modified date of the file'
        },
    ],
};

const COMPARISON_OPERATORS = [
    '=',
    '>',
    '<',
    '>=',
    '<=',
    '<>'
];

const decomposeComparisonVal = val => {
    const result = COMPARISON_OPERATORS.filter(o => val.startsWith(o));

    if (result.length === 0) {
        throw `Unknown comparison operator in ${val}`;
    }

    const operator = result[0];

    return {
        operator,
        valToCompare: val.slice(operator.length)
    };
};

const findFilter = key => {
    const functions = FILTERS.filters.filter(f => f.alias === key || `${f.column}.${f.type}` === key);

    if (functions.length === 0) {
        throw `Unknown function ${key}`;
    }

    return functions[0];
};

const getExpressionFromFilter = (filterKey, filterVal) => {
    const filter = findFilter(filterKey);

    const exp = squel.expr();

    switch (filter.type) {
        case "compare":
            const {operator, valToCompare} = decomposeComparisonVal(filterVal);
            let wrappedVal = valToCompare;

            // We need to wrap timestamp sign with the datetime value
            if (FILTER_COLUMNS[filter.column].type === COLUMN_TYPE_DATETIME) {
                wrappedVal = `timestamp '${wrappedVal}'`;
            }

            exp.and(`${filter.column} ${operator} ${wrappedVal}`);
            break;
        case "end_with":
            exp.and(`${filter.column} like \'%${filterVal}\'`);
            break;
        case "include":
            exp.and(`${filter.column} like \'%${filterVal}%\'`);
            break;
        default:
            throw `Unsupported filter type ${filter.type}`
    }

    return exp;
};

const parseFilterQueryString = queryString => {
    const filters = queryString.trim().split(' ');

    const exp = squel.expr();

    for (let i=0; i<filters.length; i++) {
        const filter = filters[i];
        const tokens = filter.split(':');
        let filterKey;

        switch (tokens.length) {
            case 1:
                filterKey = FILTERS.default;
                exp.and(getExpressionFromFilter(filterKey, tokens[0]));
                break;
            case 2:
                filterKey = tokens[0];
                exp.and(getExpressionFromFilter(filterKey, tokens[1]));
                break;
            default:
                throw `Unexpected token in ${filter}`;
        }
    }

    return exp;
};

export default parseFilterQueryString;