const {Bitrix} = require("@2bad/bitrix")
const {logError, logAccess} = require("../logger/logger");

const pageSize = 50;

class ProductsService {
    bx;

    constructor(link) {
        this.bx = Bitrix(link);
    }

    async getProductList() {
        const allResults = [];
        let res;

        let start = 0;
        let total = 0;
        try {
            do {
                res = await this.bx.call("crm.product.list",
                    {
                        "select": ["ID", "NAME"],
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
            logError("ProductsService getProductList", error);
            return null;
        }
    }

    async getProductById(productId) {
        try {
            const res = await this.bx.call("crm.product.get", {
                id: productId,
            })
            return res.result;
        } catch (error) {
            logError("ProductsService getProductById", error);
            return null;
        }
    }

    async getOriginalProductId(offerId) {
        try {
            const res = await this.bx.call("catalog.product.offer.get", {
                id: offerId,
            })
            return res.result.offer;
        } catch (error) {
            logError("ProductsService getOriginalProductId", error);
            return null;
        }
    }
}

module.exports = ProductsService;