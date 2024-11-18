const express = require('express');
const bodyParser = require('body-parser');
const cors = require("cors");
const dotenv = require('dotenv');
const path = require("path");
const timeout = require("connect-timeout");

const {logAccess, logError} = require("./logger/logger");
const Db = require("./services/db");
const {encryptText, decryptText} = require("./services/crypt");

const DealsService = require("./services/deals");
const ProductsService = require("./services/products");
const UsersService = require("./services/users");
const {appendFileSync} = require("node:fs");
const {all} = require("express/lib/application");

const envPath = path.join(__dirname, '.env');
dotenv.config({ path: envPath });

const app = express();
const PORT = 1328;

const BASE_URL = "/montajniki/"

const db = new Db();
db.createTables();

app.use(cors({
    origin: "*",
}));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(timeout('20m'));

function haltOnTimedOut(req, res, next) {
    if (!req.timedout) next();
}

// Handler for installation team members
app.post(BASE_URL+"get_deals_with_products/", async (req, res) => {
    try {
        const initiatorName = req.body.initiator_full_name;

        const db = new Db();
        const user = await db.getUserByFullName(initiatorName);

        if (!user.department_ids.includes("27")) {
            res.status(403).json({"status": false, "status_msg": "access_denied", "message": "User not allowed"});
            return;
        }
        const allDeals = (await getDealsWithProducts(user.id)).filter(deal => !deal.is_conducted && deal.is_approved);
        // const allDeals = (await getDealsWithProducts(user.id));

        res.status(200).json({"status": true, "status_msg": "success", "deals": allDeals});
    } catch (error) {
        logError(BASE_URL + "get_deals_with_products/", error);
        res.status(500).json({ "status": false, "status_msg": "error", "message": "server error" });
    }
})

// Handler for installation team members
app.post(BASE_URL+"set_fact_amount_of_products_in_deal/", async (req, res) => {
    try {
        const initiatorName = req.body.initiator_full_name;

        const db = new Db();
        const user = await db.getUserByFullName(initiatorName);

        if (!user.department_ids.includes("27")) {
            res.status(403).json({"status": false, "status_msg": "access_denied", "message": "User not allowed"});
        }

        const dealId = req.body.deal_id;
        const products = req.body.products; // Expecting an array of { product_id, fact_amount }

        // Loop through each product and update the fact amount in the local database
        for (const product of products) {
            const updateResult = await db.updateDealProductQuantities({
                deal_id: dealId,
                product_id: product.id,
                fact_amount: product.fact_amount
            });

            if (!updateResult) {
                throw new Error(`Failed to update fact_amount for product ${product.id} in deal ${dealId}`);
            }
        }

        await new Promise(resolve => setTimeout(resolve, 50));

        db.updateDealById(dealId, { "is_conducted": true });

        logAccess(BASE_URL + "set_fact_amount_of_products_in_deal/", `Fact amounts successfully updated for deal ${dealId}`);

        // Send response back to the client
        res.status(200).json({ "status": true, "status_msg": "success", "message": "Fact amounts successfully updated" });

    } catch (error) {
        logError(BASE_URL + "set_fact_amount_of_products_in_deal/", error);
        res.status(500).json({ "status": false, "status_msg": "error", "message": "server error" });
    }
});

// Handler for warehouse manager
app.post(BASE_URL+"update_deal/", async (req, res) => {
    try {
        const dealId = req.body.deal_id;
        const products = req.body.products;
        const assignedPersonalId = req.body.assigned_id;
        // UF_CRM_1730790163295

        const db = new Db();
        const bxLinkDecrypted = await decryptText(process.env.BX_LINK);

        const dealsService = new DealsService(bxLinkDecrypted);

        // Update the assigned_personal_id in the deals table
        const updateResult = db.updateDealById(dealId, { "assigned_id": assignedPersonalId, "is_approved": true });
        if (updateResult) {
            logAccess(BASE_URL + "update_deal/", `Deal ${dealId} successfully updated in db`);
        } else {
            throw new Error(`Error while updating deal ${dealId} in db`);
        }

        // Update the deal's assigned ID in the external service (Bitrix, etc.)
        if (await dealsService.updateDeal(dealId, { "UF_CRM_1728999528": assignedPersonalId, "UF_CRM_1730790163295": 1 })) {
            logAccess(BASE_URL + "update_deal/", `Deal ${dealId} successfully updated in bx`);
        }

        // Prepare the product rows for the external service
        const productRows = products.map(product => {
            return { "PRODUCT_ID": product.id, "QUANTITY": product.given_amount };
        });

        if (await dealsService.updateDealProductRows(dealId, productRows)) {
            logAccess(BASE_URL + "update_deal/", `Deal ${dealId} product rows successfully updated in bx`);
        }  else {
            throw new Error(`Error while updating product rows for deal ${dealId} in bx`);
        }

        // Update deals_products table in local database
        const productsUpdateResult = db.updateDealsProducts(dealId, products);
        if (productsUpdateResult) {
            logAccess(BASE_URL + "update_deal/", `Deal ${dealId} products successfully updated in db`);
        } else {
            throw new Error(`Error while updating products for deal ${dealId} in db`);
        }

        // Send response back to the client
        res.status(200).json({ "status": true, "status_msg": "success", "message": "Deal and products successfully updated" });

    } catch (error) {
        logError(BASE_URL + "update_deal/", error);
        res.status(500).json({ "status": false, "status_msg": "error", "message": "server error" });
    }
});

/*
* Добавить хендлер на обновление сделкив в бд по ID.
* Это не вызовет рекурсию, так как хендлер будет обновлять чисто сделку в бд.
* */

// Handler for warehouse manager
app.post(BASE_URL+"get_products_from_db/", async (req, res) => {
    try {
        const initiatorName = req.body.initiator_full_name;

        const db = new Db();
        const user = await db.getUserByFullName(initiatorName);

        if (!user.department_ids.includes("45")) {
            res.status(403).json({"status": false, "status_msg": "access_denied", "message": "User not allowed"});
            return;
        }

        const products = await db.getProducts();

        res.status(200).json({"status": true, "status_msg": "success", "data": {"products": products}});
    } catch (error) {
        logError(BASE_URL+"get_products_from_db/", error);
        res.status(500).json({"status": false, "status_msg": "error", "message": "server error"});
    }
})

// Handler for warehouse manager
app.post(BASE_URL+"get_info_for_warehouse_manager_fill_data_panel/", async (req, res) => {
    try {
        const initiatorName = req.body.initiator_full_name;

        const db = new Db();
        const user = await db.getUserByFullName(initiatorName);

        if (!user.department_ids.includes("45")) {
            res.status(403).json({"status": false, "status_msg": "access_denied", "message": "User not allowed"});
            return;
        }

        const installationDepartmentMemebers = await db.getInstallationDepartmentMembers();
        const allDeals = await getDealsWithProducts();

        res.status(200).json({"status": true, "status_msg": "success", "data": {"installation_department_memebers": installationDepartmentMemebers, "all_deals": allDeals}})

    } catch(error) {
        logError(BASE_URL+"get_info_for_warehouse_manager_fill_data_panel/", error);
        res.status(500).json({"status": false, "status_msg": "error", "message": "server error"});
    }
})

app.post(BASE_URL+"approve_deal/", async (req, res) => {
    try {
        const initiatorName = req.body.initiator_full_name;

        const db = new Db();
        const user = await db.getUserByFullName(initiatorName);

        if (!user.department_ids.includes("45")) {
            res.status(403).json({"status": false, "status_msg": "access_denied", "message": "User not allowed"});
            return;
        }

        const dealId = req.body.deal_id;
        const bxLinkDecrypted = await decryptText(process.env.BX_LINK);

        const dealsService = new DealsService(bxLinkDecrypted);
        const dealProductsFromDb = await db.getDealsProducts(dealId);
        const productRows = dealProductsFromDb.map(product => {
            return { "PRODUCT_ID": product.product_id, "QUANTITY": product.fact_amount ? product.fact_amount : product.given_amount }
        })
        if (await dealsService.updateDealProductRows(dealId, productRows)) {
            logAccess(BASE_URL + "set_fact_amount_of_products_in_deal/", `Deal ${dealId} product rows successfully updated in bx`);
        }
        res.status(200).json({"status": true, "status_msg": "success", "message": "Product rows successfully updated in BX"})
    } catch (error) {
        logError(BASE_URL+"approve_deal/", error);
        res.status(500).json({"status": false, "status_msg": "error", "message": "server error"});
    }
})

app.post(BASE_URL+"login/", async (req, res) => {
    try {
        const name = req.body.name;
        const lastName = req.body.last_name;
        const password = req.body.password;

        const fullName = name + " " + lastName;
        const userFromDb = await db.getUserByFullName(fullName);

        if (!userFromDb) {
            throw new Error(`No user ${fullName} in db`)
        }

        if (!userFromDb.password) {
            throw new Error(`no_pwd`)
        }

        if (userFromDb.name === name &&
            userFromDb.last_name === lastName &&
            userFromDb.password === password) {
            logAccess(BASE_URL + "register/", `User ${name} ${lastName} logged in`);
            res.status(200).json({"status": true, "status_msg": "success", "message": "User logged in", "user_data": {name: name, last_name: lastName}});
        } else {
            logAccess(BASE_URL + "register/", `Invalid credentials for user ${name} ${lastName}`);
            res.status(400).json({"status": false, "status_msg": "error", "message": "Invalid credentials for user"});
        }
    } catch (error) {
        logError(BASE_URL + "register/", error);
        res.status(500).json({"status": false, "status_msg": "error", "message": error.message});
    }
})

app.post(BASE_URL+"register/", async (req, res) => {
    try {
        const name = req.body.name;
        const lastName = req.body.last_name;
        const password = req.body.password;

        const bxLinkDecrypted = await decryptText(process.env.BX_LINK);

        const db = new Db();
        const usersService = new UsersService(bxLinkDecrypted);

        const user = (await usersService.getUsersListByFilter({"NAME": name, "LAST_NAME": lastName})).map(user => {
            return {
                "id": user["ID"],
                "name": user["NAME"],
                "last_name": user["LAST_NAME"],
                "departments": user["UF_DEPARTMENT"],
                "password": password
            }
        })[0];
        if (user) {
            const insertResult = db.updateUserInDb(user.id, user)
            if (insertResult) {
                logAccess(BASE_URL+"register/", `User ${user.id} ${user.name} ${user.last_name} successfully added to db`);
                res.status(200).json({"status": true, "status_msg": "success", "message": `User ${user.id} ${user.name} ${user.last_name} successfully added to db`})
            } else {
                throw new Error("Error while adding user to db");
            }
        } else {
            logError(BASE_URL + "register/", `User ${name} ${lastName} not found`)
            res.status(400).json({"status": false, "status_msg": "error", "message": `User ${name} ${lastName} not found`})
        }
    } catch (error) {
        logError(BASE_URL + "register/", error);
        res.status(500).json({"status": false, "status_msg": "error", "message": "server error"});
    }
})

app.post(BASE_URL+"check_user_permission/", async (req, res) => {
    try {
        const name = req.body.name;
        const lastName = req.body.last_name;

        const db = new Db();
        const fullName = name + " " + lastName;
        const userFromDb = await db.getUserByFullName(fullName);

        if (!userFromDb) {
            throw new Error(`No user ${fullName} in db`)
        }

        const userWarehouseManagerDeaprtmentId = userFromDb.department_ids.split(",").find(dep => Number(dep) === 45) || null;
        const userInstallationTeamDeaprtmentId = userFromDb.department_ids.split(",").find(dep => Number(dep) === 27) || null;
        if (userWarehouseManagerDeaprtmentId && userInstallationTeamDeaprtmentId) {
            res.status(200).json({"status": true, "status_msg": "success", "message": "User has permissions", "permissions": ["warehouse_manager", "installation_team"]})
        } else if (userWarehouseManagerDeaprtmentId) {
            res.status(200).json({"status": true, "status_msg": "success", "message": "User has permissions", "permissions": ["warehouse_manager"]})
        } else if (userInstallationTeamDeaprtmentId) {
            res.status(200).json({"status": true, "status_msg": "success", "message": "User has permissions", "permissions": ["installation_team"]})
        } else {
            throw new Error(`User ${fullName} doesn't have permission`);
        }
    } catch (error) {
        logError(BASE_URL + "check_user_permission/", error);
        res.status(500).json({"status": false, "status_msg": "error", "message": "server error"});
    }
})

app.post(BASE_URL+"add_deal_handler/", async (req, res) => {
    try {
        const dealId = req.body["data[FIELDS][ID]"];
        const bxLinkDecrypted = await decryptText(process.env.BX_LINK);

        const db = new Db();
        const dealService = new DealsService(bxLinkDecrypted);
        const productService = new ProductsService(bxLinkDecrypted);

        const newDeal = [(await dealService.getDealById(dealId))].map(deal => {
            return {
                id: deal["ID"],
                title: deal["TITLE"],
                date_create: deal["DATE_CREATE"],
                assigned_id: deal["UF_CRM_1728999528"] || null,
            }
        });
        if (!newDeal[0].assigned_id) {
            logError(BASE_URL+"add_deal_handler", `Deal ${newDeal[0].id} doesn't have assigned id`);
            res.status(400).json({"status": false, "status_msg": "error", "message": `Deal ${newDeal[0].id} doesn't have assigned id`});
            return;
        }

        let insertResult = db.insertDealsInDb(newDeal);
        if (insertResult) {
            logAccess(BASE_URL+"add_deal_handler", `Deal ${dealId} successfully added to db`);
        } else {
            throw new Error(`Error while deal ${dealId} in db`)
        }

        const productRows = (await dealService.getDealProductRowsByDealId(dealId)).map(pr => {
            return {
                "product_id": Number(pr["PRODUCT_ID"]),
                "given_amount": Number(pr["QUANTITY"])
            }
        });
        const products = [];
        for (let pr of productRows) {
            const originalProduct = await productService.getOriginalProductId(pr.product_id);
            if (originalProduct && Object.keys(originalProduct).length > 0) {
                products.push(
                    {
                        deal_id: dealId,
                        product_id: originalProduct.parentId.value,
                        given_amount: pr.given_amount,
                        fact_amount: null
                    }
                );
            } else {
                products.push(
                    {
                        deal_id: dealId,
                        product_id: pr.product_id,
                        given_amount: pr.given_amount,
                        fact_amount: null
                    }
                );
            }
        }
        const dealProducts = products.map(pr => {
            return {
                "deal_id": dealId,
                "product_id": pr.product_id,
                "given_amount": pr.given_amount,
                "fact_amount": null
            }
        })

        insertResult = db.insertDealsProductsInDb(dealProducts);
        if (insertResult) {
            logAccess(BASE_URL+"add_deal_handler/", `Product rows for deal ${dealId} successfully added to db`);
        } else {
            throw new Error(`Error while adding product rows for deal ${dealId} in db`)
        }

    } catch (error) {
        logError(BASE_URL+"add_deal_handler/", error);
        res.status(500).json({"status": false, "status_msg": "error", "message": "server error"})
    }
})

app.post(BASE_URL+"add_product_handler/", async (req, res) => {
    try {
        const productId = req.body["data[FIELDS][ID]"];

        const bxLinkDecrypted = await decryptText(process.env.BX_LINK);

        const db = new Db();
        const productsService = new ProductsService(bxLinkDecrypted);

        const newProduct = (await productsService.getProductById(productId)).map(product => {
            return {
                "id": product["ID"],
                "name": product["NAME"],
            }
        });

        const insertResult = db.insertProductsInDb(newProduct);
        if (insertResult) {
            logAccess(BASE_URL+"add_product_handler/", `Product ${productId} successfully added to db`);
            res.status(200).json({"status": true, "status_msg": "success", "message": `Product ${productId} successfully added to db`});
        } else {
            throw new Error(`Error while adding product ${productId} in db`);
        }
    } catch (error) {
        logError(BASE_URL+"add_deal_handler/", error);
        res.status(500).json({"status": false, "status_msg": "error", "message": "server error"})
    }
})

app.post(BASE_URL+"get_from_bx_insert_users_in_db/", async (req, res) => {
    try {
        const filter = req.body.filter;

        const bxLink = await decryptText(process.env.BX_LINK)

        const db = new Db();

        const usersService = new UsersService(bxLink);
        const users = (await usersService.getUsersListByFilter(filter)).map(user => {
            return {"id": user["ID"], "name": user["NAME"], "last_name": user["LAST_NAME"], "departments": user["UF_DEPARTMENT"]}
        });

        const insertResult = db.insertUsersInDb(users)
        if (insertResult) {
            logAccess(BASE_URL+"get_from_bx_insert_users_in_db/", "Users successfully added to db")
            res.status(200).json({"status": true, "status_msg": "success", "message": "Users successfully added to db"});
        } else {
            throw new Error("Error while inserting users in db");
        }
    } catch (error) {
        logError(BASE_URL + "get_from_bx_insert_users_in_db/", error);
        res.status(500).json({"status": false, "status_msg": "error", "message": "server error"});
    }
})

app.post(BASE_URL+"get_from_bx_insert_products_in_db/", async (req, res) => {
    try {
        const db = new Db();
        const bxLinkDecrypted = await decryptText(process.env.BX_LINK);

        const productsService = new ProductsService(bxLinkDecrypted);
        const products = (await productsService.getProductList()).map(product => {
            return {
                id: product["ID"],
                name: product["NAME"]
            }
        });

        const insertResult = db.insertProductsInDb(products);

        if (insertResult) {
            logAccess(BASE_URL+"get_from_bx_insert_products_in_db/", "Products successfully added to db");
            res.status(200).json({"status": true, "status_msg": "success", "message": "Products successfully added to db"});
        } else {
            throw new Error("Error while adding products in db");
        }
    } catch (error) {
        logError(BASE_URL + "get_from_bx_insert_products_in_db/", error);
        res.status(500).json({"status": false, "status_msg": "error", "message": "server error"});
    }
})

app.post(BASE_URL+"get_from_bx_insert_deals_in_db/", async (req, res) => {
    try {
        const filter = req.body.filter;

        const bxLink = await decryptText(process.env.BX_LINK)

        const db = new Db();
        const dealsService = new DealsService(bxLink);
        const deals = (await dealsService.getDealsListByFilter(filter)).map(deal => {
            if (deal["UF_CRM_1728999528"]) {
                return {
                    id: deal["ID"],
                    title: deal["TITLE"],
                    date_create: deal["DATE_CREATE"],
                    assigned_id: deal["UF_CRM_1728999528"]
                }
            }
        }).filter(deal => deal !== undefined);
        const insertResult = db.insertDealsInDb(deals);
        if (insertResult) {
            logAccess(BASE_URL+"get_from_bx_insert_deals_in_db/", "Deals successfully added to db");
            res.status(200).json({"status": true, "status_msg": "success", "message": "Deals successfully added to db"});
        } else {
            throw new Error("Error while adding deals in db");
        }

    } catch (error) {
        logError(BASE_URL + "get_from_bx_insert_deals_in_db/", error);
        res.status(500).json({"status": false, "status_msg": "error", "message": "server error"});
    }
})

app.post(BASE_URL+"get_from_bx_insert_deals_products_in_db/", async (req, res) => {
    try {
        const db = new Db();
        const dealsFromDb = await db.getDeals();

        const bxLinkDecrypted = await decryptText(process.env.BX_LINK);
        const dealsService = new DealsService(bxLinkDecrypted);
        const productService = new ProductsService(bxLinkDecrypted);

        let dealProducts = [];
        for (const deal of dealsFromDb) {
            const productRows = (await dealsService.getDealProductRowsByDealId(deal.id)).map(pr => {
                return {
                    "product_id": Number(pr["PRODUCT_ID"]),
                    "given_amount": Number(pr["QUANTITY"])
                }
            });
            for (let pr of productRows) {
                const originalProduct = await productService.getOriginalProductId(pr.product_id);
                if (Object.keys(originalProduct).length > 0) {
                    dealProducts.push(
                        {
                            deal_id: deal.id,
                            product_id: originalProduct.parentId.value,
                            given_amount: pr.given_amount,
                            fact_amount: null
                        }
                    );
                } else {
                    dealProducts.push(
                        {
                            deal_id: deal.id,
                            product_id: pr.product_id,
                            given_amount: pr.given_amount,
                            fact_amount: null
                        }
                    );
                }
            }
        }

        const insertResult = db.insertDealsProductsInDb(dealProducts);
        if (insertResult) {
            logAccess(BASE_URL+"get_from_bx_insert_deals_products_in_db/", "DealProducts successfully added to db");
            res.status(200).json({"status": true, "status_msg": "success", "message": "DealProducts successfully added to db"});
        } else {
            throw new Error("Error while adding deals in db");
        }
    } catch (error) {
        logError(BASE_URL + "get_from_bx_insert_deals_products_in_db/", error);
        res.status(500).json({"status": false, "status_msg": "error", "message": "server error"});
    }
})

app.post(BASE_URL+"delete_deal_handler", async (req,res) => {
    try {
        let id = req.query["ID"];
        if (!id) {
            id = req.body["data[FIELDS][ID]"];
        }
        if (!id) {
            logError(BASE_URL+"delete_deal_handler", "No deal id provided")
            res.status(400).json({"status": false, "status_msg": "error", "message": "No deal id provided"});
            return;
        }

        const db = new Db();
        const deleteDealResult =  db.deleteDealById(id);
        if (deleteDealResult) {
            const deleteDealProductsResult = db.deleteDealsProductsRowByDealId(id);
            if (deleteDealProductsResult) {
                logAccess(BASE_URL+"delete_deal_handler", `Deal ${id} successfully deleted`)
                res.status(200).json({"status": true, "status_msg": "success", "message": `Deal ${id} successfully deleted`});
            }
        }
    } catch (error) {
        logError(BASE_URL+"delete_deal_handler", error)
        res.status(500).json({"status": false, "status_msg": "error", "message": "server  error"})
    }
})

app.post(BASE_URL+"tmp/", async (req, res) => {
    const db = new Db();
    res.status(200).json();
})

async function getDealsWithProducts(assigned_id = null) {
    const allDeals = await db.getDeals(assigned_id);
    const allProducts = await db.getProducts();
    const dealProducts = await db.getDealsProducts();

    const dealsWithProducts = allDeals.map(deal => {
        // Find products associated with the current deal by matching IDs
        const productsInDeal = dealProducts
            .filter(dp => dp.deal_id === deal.id)
            .map(dp => {
                const product = allProducts.find(p => p.id === dp.product_id);
                if (!product) {
                    logError("getDealsWithProducts", `Product with ID ${dp.product_id} not found. Deal - ${deal.id}`);
                    return null;  // Skip or handle as necessary
                }
                return {
                    id: product.id,
                    name: product.name,
                    given_amount: dp.given_amount,
                    fact_amount: dp.fact_amount,
                    total: dp.total
                };
            })
            .filter(product => product !== null); // Remove null entries

        return { ...deal, products: productsInDeal };
    });

    return dealsWithProducts;
}

app.listen(PORT, () => {
    console.log(`Server is running on port: ${PORT}`);
});
