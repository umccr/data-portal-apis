import * as squel from "squel";
import {badRequest, failure, success} from "./libs/response-lib";
import * as athena from "./libs/athena-lib";

const rowsPerPage = 20;

export const main = async (event, context) => {
    const searchString = event.queryStringParameters.query;

    if (searchString === null) {
        return badRequest({errors: 'query cannot be empty'});
    }

    const pageString = event.queryStringParameters.page;
    // Default index is 0
    const page = pageString ? parseInt(pageString) : 0;

    const innerQueryBuilder = squel
        .select()
        .field('row_number() over()', 'rn')
        .field('*')
        .from('data');

    const rowStart = rowsPerPage * page + 1;
    const rowEnd = rowsPerPage * (page+1);

    const paginationExpression = squel
        .expr()
        .and('rn BETWEEN ? AND ?', rowStart, rowEnd);

    const searchExpression = squel
        .expr()
        .and(`key like '%${searchString}%'`);

    const dataQuery = squel
        .select()
        .from(innerQueryBuilder)
        .where(squel.expr().and(paginationExpression).and(paginationExpression));

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
    const headerRow = rows[0].Data.map(col => col.VarCharValue);
    const dataRows = rows.slice(1).map(row => row.Data.map(col => col.VarCharValue));
    return { headerRow, dataRows };
};

const getTotal = rows => {
    return parseInt(rows[1].Data[0].VarCharValue);
};
