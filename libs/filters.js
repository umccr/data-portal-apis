import * as squel from "squel";
import {getDataForQuery, LIMS_TABLE_NAME} from "./athena-lib";
import {processResultRows} from "../file-search";

const COLUMN_TYPE_STRING = 'string';
const COLUMN_TYPE_INTEGER = 'integer';
const COLUMN_TYPE_DATETIME = 'datetime';
const COLUMN_TYPE_ILLUMINA = 'illumina';

const FILTER_COLUMNS = {
    key: {
        type: COLUMN_TYPE_STRING
    },
    size: {
        type: COLUMN_TYPE_INTEGER
    },
    last_modified_date: {
        type: COLUMN_TYPE_DATETIME
    },
    illumina_id: {
        type: COLUMN_TYPE_ILLUMINA
    }
};

const FILTER_TYPE_END_WITH = 'end_with';
const FILTER_TYPE_INCLUDE = 'include';
const FILTER_TYPE_COMPARE = 'compare';
const FILTER_TYPE_GLOBAL = 'global';

const FILTERS = {
    default: 'key.include',
    filters: [
        {
            column: 'key',
            type: FILTER_TYPE_END_WITH,
            alias: 'ext',
            description: 'File extension'
        },
        {
            column: 'key',
            type: FILTER_TYPE_INCLUDE,
            alias: 'pathinc',
            description: 'File path includes'
        },
        {
            column: 'size',
            type: FILTER_TYPE_COMPARE,
            alias: 'size',
            description: 'Compare with file size'
        },
        {
            column: 'last_modified_date',
            type: FILTER_TYPE_COMPARE,
            alias: 'date',
            description: 'Compare with last modified date of the file'
        },
        {
            column: 'illumina_id',
            type: FILTER_TYPE_INCLUDE,
            alias: 'illumina_id',
            description: 'Illumina_id (in LIMS table) includes'
        },
        {
            column: 'case',
            type: FILTER_TYPE_GLOBAL,
            alias: 'case',
            description: 'Defines case sensitivity for string comparison. Default to false'
        }
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

const get_key_patterns_from_lims = async illumina_id => {
    const query = squel.select()
        .fields(['sampleid', 'samplename', 'project'])
        .from(LIMS_TABLE_NAME)
        .where(`illumina_id LIKE \'%${illumina_id}%\'`);

    console.log(query.toString());

    const patterns = await getDataForQuery(query.toString(), rows => {
        const dataRows = rows.slice(1).map(row => row.Data.map(col => col.VarCharValue));
        const patterns = [];

        dataRows.map(row => {
            // Use sample id and sample name for now
            patterns.push(row[0]);
            patterns.push(row[1]);
        });

        return patterns;
    });

    return patterns;
};

const LIKE_INCLUDE = 'include';
const LIKE_START_WITH = 'start_with';
const LIKE_END_WITH = 'end_with';
const LIKE_EQUAL = 'equal';

const getLikeExpression = (col, val, mode, caseSensitive) => {
    let wrappedVal;

    switch (mode) {
        case LIKE_INCLUDE:
            wrappedVal = `%${val}%`;
            break;
        case LIKE_START_WITH:
            wrappedVal = `${val}%`;
            break;
        case LIKE_END_WITH:
            wrappedVal = `%${val}`;
            break;
        case LIKE_EQUAL:
            wrappedVal = val;
    }

    wrappedVal = `'${wrappedVal}'`

    const formattedCol = caseSensitive ? col : `LOWER(${col})`;
    const formattedVal = caseSensitive ? wrappedVal : `LOWER(${wrappedVal})`;

    return `${formattedCol} LIKE ${formattedVal}`;
};

const getExpressionFromFilter = async (filterKey, filterVal, isCaseSensitive) => {
    const filter = findFilter(filterKey);

    const exp = squel.expr();

    switch (filter.type) {
        case FILTER_TYPE_COMPARE:
            const {operator, valToCompare} = decomposeComparisonVal(filterVal);
            let wrappedVal = valToCompare;

            // We need to wrap timestamp sign with the datetime value
            if (FILTER_COLUMNS[filter.column].type === COLUMN_TYPE_DATETIME) {
                wrappedVal = `timestamp '${wrappedVal}'`;
            }

            exp.and(`${filter.column} ${operator} ${wrappedVal}`);
            break;
        case FILTER_TYPE_END_WITH:
            exp.and(getLikeExpression(filter.column, filterVal, LIKE_END_WITH, isCaseSensitive));
            break;
        case FILTER_TYPE_INCLUDE:
            // Special case for LIMS Illumina filtering
            if (FILTER_COLUMNS[filter.column].type === COLUMN_TYPE_ILLUMINA) {
                const patterns = await get_key_patterns_from_lims(filterVal);

                if (patterns.length === 0) {
                    throw `illumina_id ${filterVal} not found`;
                }

                const innerExp = squel.expr();

                // We only need at least one pattern to be matched
                for (let i=0; i<patterns.length; i++) {
                    innerExp.or(getLikeExpression(key, patterns[i], LIKE_INCLUDE, isCaseSensitive))
                }

                exp.and(innerExp);
            } else {
                // Normal include case
                exp.and(getLikeExpression(filter.column, filterVal, LIKE_INCLUDE, isCaseSensitive))
            }

            break;
        default:
            throw `Unsupported filter type ${filter.type}`;
    }

    return exp;
};

const parseFilterQueryString = async queryString => {
    const filters = queryString.trim().split(' ');
    const exp = squel.expr();
    const filterConfigs = [];

    // Check through all filter strings first
    for (let i=0; i<filters.length; i++) {
        const filter = filters[i];
        const tokens = filter.split(':');
        let filterKey, filterVal;

        switch (tokens.length) {
            case 1:
                // case {filterVal} (default filter)
                filterKey = FILTERS.default;
                filterVal = tokens[0];
                break;
            case 2:
                // case {filterKey}:{filterVal}
                filterKey = tokens[0];
                filterVal = tokens[1];
                break;
            default:
                throw `Unexpected token in ${filter}`;
        }

        // Append filter config
        filterConfigs.push({
            key: filterKey,
            val: filterVal
        });
    }

    // Find config for case sensitivity first, if it is true, then it is case sensitive; default to false
    const caseSensitivityFilterIndex = filterConfigs.findIndex(f => f.key === 'case');
    const isCaseSensitive = caseSensitivityFilterIndex >= 0
        && filterConfigs[caseSensitivityFilterIndex].val === 'true';

    for (let i=0; i<filterConfigs.length; i++) {
        if (i === caseSensitivityFilterIndex) {
            // Ignore global filter
            continue;
        }

        const filterObject = filterConfigs[i];
        // Wait til filter has been loaded and append this to our filter expression
        exp.and(await getExpressionFromFilter(filterObject.key, filterObject.val, isCaseSensitive));
    }

    return exp;
};

export default parseFilterQueryString;