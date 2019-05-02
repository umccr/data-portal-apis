import * as squel from "squel";
import {badRequest, failure, success} from "./libs/response-lib";
import * as athena from "./libs/athena-lib";

const DEFAULT_ROWS_PER_PAGE = 20;
const SORTABLE_COLUMNS = ['size', 'last_modified_date'];

export const main = async (event, context) => {
    const queryStringParams = event.queryStringParameters;
    const filePath = queryStringParams.filePath;

    if (filePath === null) {
        return badRequest({errors: 'query cannot be empty'});
    }

    const pageString = queryStringParams.page;
    // Default index is 0
    const page = pageString ? parseInt(pageString) : 0;
    const rowsPerPageString = queryStringParams.rowsPerPage;
    const rowsPerPage = rowsPerPageString ? parseInt(rowsPerPageString) : DEFAULT_ROWS_PER_PAGE;

    const sortCol = queryStringParams.sortCol;
    const sortAsc = queryStringParams.sortAsc === true || queryStringParams.sortAsc === 'true';

    const sortQueryString = sortCol ? `ORDER BY ${sortCol} ${sortAsc ? 'ASC' : 'DESC'}` : '';

    const rowStart = rowsPerPage * page + 1;
    const rowEnd = rowsPerPage * (page+1);

    const paginationExpression = squel
        .expr()
        .and('rn BETWEEN ? AND ?', rowStart, rowEnd);

    const searchExpression = squel
        .expr()
        .and(`key like '%${filePath}%'`);

    if (queryStringParams.fileExtension) {
        searchExpression.and(`key like '%.${queryStringParams.fileExtension}'`);
    }

    const innerQuery = squel
        .select()
        .field(`row_number() over(${sortQueryString})`, 'rn')
        .field('bucket')
        .field('key')
        .field('size')
        .field('last_modified_date')
        .from('data')
        .where(searchExpression);

    const dataQuery = squel
        .select()
        .from(innerQuery)
        .where(paginationExpression);

    const metaDataQuery = squel
        .select()
        .field('COUNT()', 'total')
        .from('data')
        .where(searchExpression);

    try {
        console.log(dataQuery.toString());
        const rows = await athena.getDataForQuery(dataQuery.toString(), processResultRows);

        console.log(metaDataQuery.toString());
        const total = await athena.getDataForQuery(metaDataQuery.toString(), getTotal);

        const metaData = {
            size: rowsPerPage,
            page: page + 1,
            start: rowStart,
            totalRows: total,
            totalPages: Math.floor(total/rowsPerPage) + 1
        };

        return success({rows: rows, meta: metaData});
    } catch (e) {
        return failure({status: false, errors: e});
    }
};

const processResultRows = rows => {
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
