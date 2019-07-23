import * as squel from "safe-squel";
import {badRequest, success} from "./libs/response-lib";
import * as athena from "./libs/athena-lib";
import {S3_KEYS_TABLE_NAME} from "./libs/athena-lib";
import parseFilterQueryString from "./libs/filters";

const DEFAULT_PAGE_NUMBER = 0;
const DEFAULT_ROWS_PER_PAGE = 20;
const SORTABLE_COLUMNS = ['size', 'last_modified_date'];

// Maximum allowed percentage for sub sampling
const MAX_RAND_SAMPLES_LIMIT = 500;

const ROW_NUMBER_ALIAS = 'rn';

/**
 * event.queryStringParameters:
 * {
 *      query:          string, mandatory,  the query string (leave empty for no filter)
 *      rowsPerPage:    int,    optional,   number of rows per page
 *      page:           int,    opti onal,   the current page number
 *      sortCol:        string, optional,   the column to be sorted
 *      sortAsc:        bool,   optional,   sort in ascending order
 *      randomSamples:  int,    optional,   retrieve n randomly-selected samples
 * }
 */
export const main = async (event, context) => {
    try {
        return await processSearchRequest(event);
    } catch (e) {
        let message = e;

        if (typeof e === "object" && e.code) {
            message = 'Unknown error: ' + e.code
        }

        return badRequest({status: false, errors: message});
    }
};

const getRowsPerPage = params => {
    const { rowsPerPage } = params;
    return rowsPerPage ? parseInt(rowsPerPage) : DEFAULT_ROWS_PER_PAGE;
};

const getPageNumber = params => {
    const { page } = params;
    return page ? parseInt(page) : DEFAULT_PAGE_NUMBER;
};

const getFilterQueryString = params => {
    const { query } = params;

    if (query === undefined) {
        throw 'search query cannot be empty';
    }

    return query
};

const getSortQueryString = params => {
    let { sortCol, sortAsc } = params;

    sortAsc = sortAsc === true || sortAsc === 'true';

    return sortCol ? `ORDER BY ${sortCol} ${sortAsc ? 'ASC' : 'DESC'}` : '';
};

const getPagination = (rowsPerPage, page) => {
    const rowStart = rowsPerPage * page + 1;
    const rowEnd = rowsPerPage * (page+1);

    return {
        rowStart,
        rowEnd,
        paginationExpression: squel
        .expr()
        .and('rn BETWEEN ? AND ?', rowStart, rowEnd)
    }
};

const getRandomSamplingConfig = params => {
    const { randomSamples } = params;
    let parsedInt = null;

    if (randomSamples) {
        parsedInt = parseInt(randomSamples);

        if (isNaN(parsedInt)) {
            throw 'Random sampling limit must be an integer';
        }

        if (parsedInt < 1 || parsedInt > MAX_RAND_SAMPLES_LIMIT) {
            throw `Random samples limit is not in the valid range: 1~${MAX_RAND_SAMPLES_LIMIT}`
        }
    }

    return parsedInt;
};

const processSearchRequest = async event => {
    const { queryStringParameters } = event;
    const params = queryStringParameters;

    const filterQueryString = getFilterQueryString(params);
    const page = getPageNumber(params);
    let rowsPerPage = getRowsPerPage(params);

    const sortQueryString = getSortQueryString(params);

    const searchExpression = await parseFilterQueryString(filterQueryString);

    // Compose query for retrieving meta data
    const metaDataQuery = squel
        .select()
        .field('COUNT()', 'total')
        .from(S3_KEYS_TABLE_NAME)
        .where(searchExpression);

    let tableRowCount = null;

    // Check whether we want random samples
    const randomSamples = getRandomSamplingConfig(params);

    // By default we dont do random sampling
    let innerQueryFrom = S3_KEYS_TABLE_NAME;
    let randPercentage;

    if (randomSamples !== null) {
        // For random sampling, default rows per page doesn't apply
        rowsPerPage = MAX_RAND_SAMPLES_LIMIT;

        // Calculate sampling percentage based on current table row count
        tableRowCount = await athena.getDataForQuery(metaDataQuery.toString(), getTotal);
        randPercentage = (randomSamples / tableRowCount) * 100;

        console.log(randPercentage);
        // Append sub sampling to the from table name
        innerQueryFrom = `${S3_KEYS_TABLE_NAME} TABLESAMPLE BERNOULLI (${randPercentage})`;
    }

    // Compose pagination expression after random sampling config has been read
    const {rowStart, paginationExpression} = getPagination(rowsPerPage, page);

    const innerQuery = squel
        .select()
        .field(`row_number() over(${sortQueryString})`, ROW_NUMBER_ALIAS)
        .field('bucket')
        .field('key')
        .field('CONCAT(\'s3://\', bucket, \'/\', key) AS path')
        .field('size')
        .field('last_modified_date')
        .from(innerQueryFrom)
        .where(searchExpression);

    const dataQuery = squel
        .select()
        .from(innerQuery)
        .where(paginationExpression)
        .order(ROW_NUMBER_ALIAS, true);

    console.log(dataQuery.toString());
    const rows = await athena.getDataForQuery(dataQuery.toString(), processResultRows);

    // Only retrieve table row count if we haven't got it yet.
    if (tableRowCount === null) {
        console.log(metaDataQuery.toString());
        tableRowCount = await athena.getDataForQuery(metaDataQuery.toString(), getTotal);
    }

    let metaData;

    // Give meaningful meta based on request type
    if (randomSamples) {
        metaData = {
            totalRows: tableRowCount,
            randPercentage
        };
    } else {
        metaData = {
            size: rowsPerPage,
            page: page + 1,
            start: rowStart,
            totalRows: tableRowCount,
            totalPages: Math.floor(tableRowCount/rowsPerPage) + 1
        };
    }

    return success({rows: rows, meta: metaData});
};

export const processResultRows = rows => {
    const headerRow = rows[0].Data.map(col => {
        const val = col.VarCharValue;

        return {
            key: val,
            sortable: SORTABLE_COLUMNS.includes(val)
        }
    });
    const dataRows = rows.slice(1).map(row => row.Data.map(col => col.VarCharValue));

    return { headerRow, dataRows };
};

const getTotal = rows => {
    return parseInt(rows[1].Data[0].VarCharValue);
};
