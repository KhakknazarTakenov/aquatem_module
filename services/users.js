const {Bitrix} = require("@2bad/bitrix")
const {logError, logAccess} = require("../logger/logger");

const pageSize = 50;

class UsersService {
    bx;

    constructor(link) {
        this.bx = Bitrix(link);
    }

    async getUsersListByFilter(filter = {}) {
        const allResults = [];
        let res;

        let start = 0;
        let total = 0;
        try {
            do {
                res = await this.bx.call("user.search",
                    {
                        "select": ["ID", "NAME", "LAST_NAME", "UF_DEPARTMENT"],
                        "FILTER": filter,
                        "start": start
                    }
                )

                total = res.total;
                start += pageSize;

                allResults.push(...res.result);
            } while(start < total)

            return allResults;
        } catch (error) {
            logError("ProductsService getUsersList", error);
            return null;
        }
    }

    async getUserByFilter(filter = {}) {
        try {
            const res = await this.bx.call("user.search",
                {
                    "select": ["ID", "NAME", "LAST_NAME", "UF_DEPARTMENT"],
                    "filter": filter,
                }
            )

            return res;
        } catch (error) {
            logError("ProductsService getUsersList", error);
            return null;
        }
    }
}

module.exports = UsersService;