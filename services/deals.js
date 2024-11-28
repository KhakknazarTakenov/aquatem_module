const {Bitrix} = require("@2bad/bitrix")
const {logError, logAccess} = require("../logger/logger");

const pageSize = 50;

class DealsService {
    bx;

    constructor(link) {
        this.bx = Bitrix(link);
    }

    async getDealsListByFilter(filter = {}) {
        const allResults = [];
        let res;

        let start = 0;
        let total = 0;
        try {
            do {
                res = await this.bx.deals.list(
                    {
                        "select": ["ID", "TITLE", "CATEGORY_ID", "UF_CRM_1728999194580", "UF_CRM_1728999528", "UF_CRM_1732081124429", "UF_CRM_1732531742220"],
                        "filter": filter,
                        "start": start
                    }
                )

                total = res.total;
                start += pageSize;

                allResults.push(...res.result);
                if (res.total < pageSize) {
                    break;
                }
            } while(start < total)
            return allResults;
        } catch (error) {
            logError("ProductsService getDealsListByFilter", error);
            return null;
        }
    }

    async getDealProductRowsByDealId(dealId) {
        const allResults = [];
        let res;

        let start = 0;
        let total = 0;
        try {
            do {
                res = await this.bx.call("crm.deal.productrows.get",
                    {
                        "id": dealId,
                    }
                )

                total = res.total;
                start += pageSize;
                allResults.push(...res.result);

                if (res.total < pageSize) {
                    break;
                }
            } while(start < total)

            return allResults;
        } catch (error) {
            logError("ProductsService getDealProductRowsByDealId", error + ` DEAL ID - ${dealId}`);
            return null;
        }
    }

    async getDealById(dealId) {
        try {
            return (await this.bx.deals.get(dealId)).result;
        } catch (error) {
            logError("DealsService getDealById", error);
        }
    }

    async updateDeal(dealId, updatingFields = {}) {
        try {
            await this.bx.call("crm.deal.update", {
                id: dealId,
                fields: updatingFields
            });
            return true;
        } catch (error) {
            logError("DealsService updateDeal", error);
            return false;
        }
    }

    async updateDealProductRows(dealId, productRows = []) {
        try {
            const res = await this.bx.call("crm.deal.productrows.set", {
                id: dealId,
                rows: productRows
            });
            return true;
        } catch (error) {
            logError("DealsService updateDeal", error);
            return false;
        }
    }
}

module.exports = DealsService;