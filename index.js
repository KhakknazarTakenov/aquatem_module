const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const dotenv = require("dotenv");
const path = require("path");
const timeout = require("connect-timeout");

const { logAccess, logError } = require("./logger/logger");
const Db = require("./services/db");
const { encryptText, decryptText } = require("./services/crypt");

const DealsService = require("./services/deals");
const ProductsService = require("./services/products");
const UsersService = require("./services/users");
const { appendFileSync } = require("node:fs");
const { all } = require("express/lib/application");

const envPath = path.join(__dirname, ".env");
dotenv.config({ path: envPath });

const app = express();
const PORT = 1328;

const BASE_URL = "/montajniki/";

const db = new Db();
db.createTables();

app.use(
    cors({
        origin: "*",
    })
);

app.post(
    BASE_URL + "add_comment/",
    express.json({ limit: '20mb' }),
    async (req, res) => {
        try {
            const userID = req.body.user_id;
            const dealId = req.body.deal_id;
            const comment = req.body.comment;
            const picturesArr = req.body.picturesArr;
            // console.log(req.body);

            const bxLinkDecrypted = await decryptText(process.env.BX_LINK);

            const dealsService = new DealsService(bxLinkDecrypted);
            // Update the deal's assigned ID in the external service (Bitrix, etc.)
            // const picturesArr =

            if (
                await dealsService.updateDealWithPicture(dealId, { UF_CRM_1740318723: comment }, { UF_CRM_1740324915: picturesArr})
            ) {
                logAccess(
                    BASE_URL + "add_comment/",
                    `Deal ${dealId} successfully updated (added comment) in bx`
                );
            }

            if (
                await dealsService.addPictureToTaskComment(dealId, {
                    comment: comment,
                    userID: userID,
                })
            ) {
                logAccess(
                    BASE_URL + "add_comment/",
                    `Deal ${dealId} successfully updated (added picture to task comment) in bx`
                );
            }

            res.status(200).json({
                status: true,
                status_msg: "success",
                message: "Deal and task successfully updated",
            });
        } catch (error) {
            logError(BASE_URL + "add_comment/", error);
            res
                .status(500)
                .json({ status: false, status_msg: "error", message: "server error" });
        }
    });

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));

// app.use(express.json({ limit: '20mb' })); // Увеличиваем лимит на 20MB
// app.use(express.urlencoded({ extended: true, limit: '20mb' })); // Для form-data
app.use(timeout("20m"));

function haltOnTimedOut(req, res, next) {
    if (!req.timedout) next();
}

app.post(BASE_URL + 'set_user_sign/', express.json({ limit: '20mb' }), async (req, res) => {
    try {
        const { dealId, image } = req.body; // Получаем dealId и image из тела запроса

        // Проверяем, что данные переданы
        if (!dealId || !image) {
            return res.status(400).json({
                status: false,
                status_msg: 'error',
                message: 'Не переданы dealId или изображение'
            });
        }

        const bxLinkDecrypted = await decryptText(process.env.BX_LINK);

        // Создаем экземпляр DealsService с твоим вебхуком
        const dealsService = new DealsService(bxLinkDecrypted);

        // Вызываем метод для загрузки изображения в пользовательское поле
        const result = await dealsService.uploadPictureToDealUserField(dealId, image);

        if (result) {
            res.status(200).json({
                status: true,
                status_msg: 'success',
                message: 'Изображение успешно добавлено в сделку'
            });
        } else {
            throw new Error('Не удалось обновить сделку');
        }
    } catch (error) {
        logError(BASE_URL + 'set_user_sign/', error);
        res.status(500).json({
            status: false,
            status_msg: 'error',
            message: 'Сервер вернул ошибку при попытке загрузить изображение в сделку'
        });
    }
});

// Handler for installation team members
app.post(BASE_URL + "get_deals_with_products/", async (req, res) => {
    try {
        const initiatorName = req.body.initiator_full_name;

        const db = new Db();
        const user = await db.getUserByFullName(initiatorName);

        if (!user.department_ids.includes("27")) {
            res.status(403).json({
                status: false,
                status_msg: "access_denied",
                message: "User not allowed",
            });
            return;
        }
        // console.log(await getDealsWithProducts(user.id));

        const allDeals = (await getDealsWithProducts(user.id)).filter(
            (deal) =>
                deal.is_moved &&
                !deal.is_approved &&
                !deal.is_failed &&
                !deal.is_conducted
        );
        // const allDeals = (await getDealsWithProducts(user.id));
        // console.log(allDeals);
        res
            .status(200)
            .json({ status: true, status_msg: "success", deals: allDeals, user_id: user.id });
    } catch (error) {
        logError(BASE_URL + "get_deals_with_products/", error);
        res
            .status(500)
            .json({ status: false, status_msg: "error", message: "server error" });
    }
});

// Handler for installation team members
app.post(
    BASE_URL + "set_fact_amount_of_products_in_deal/",
    async (req, res) => {
        try {
            const initiatorName = req.body.initiator_full_name;
            // console.log(req.body);

            const db = new Db();
            const user = await db.getUserByFullName(initiatorName);

            if (!user.department_ids.includes("27")) {
                res.status(403).json({
                    status: false,
                    status_msg: "access_denied",
                    message: "User not allowed",
                });
            }

            const dealId = req.body.deal_id;
            const products = req.body.products; // Expecting an array of { product_id, fact_amount }
            const servicePrice = req.body.service_price;

            if (
                db.updateDealById(dealId, {
                    service_price: servicePrice,
                    is_conducted: true,
                })
            ) {
                logAccess(
                    BASE_URL + "set_fact_amount_of_products_in_deal/",
                    `Deal ${dealId} service price updated successfully in db`
                );
            }
            // console.log("Строка 115, Материалы:", products);
            // Loop through each product and update the fact amount in the local database
            for (const product of products) {
                const updateResult = await db.updateDealProductQuantities({
                    deal_id: dealId,
                    product_id: product.id,
                    fact_amount: product.fact_amount,
                    given_amount: product.given_amount,
                });

                if (!updateResult) {
                    throw new Error(
                        `Failed to update fact_amount for product ${product.id} in deal ${dealId}`
                    );
                }
            }

            await new Promise((resolve) => setTimeout(resolve, 50));

            const bxLinkDecrypted = await decryptText(process.env.BX_LINK);

            const dealsService = new DealsService(bxLinkDecrypted);

            if (
                await dealsService.updateDeal(dealId, {
                    UF_CRM_1732531742220: servicePrice,
                })
            ) {
                logAccess(
                    BASE_URL + "update_deal/",
                    `Deal ${dealId} service price updated successfully in bx`
                );
            }

            const productRows = products.map((product) => {
                return {
                    PRODUCT_ID: product.id,
                    QUANTITY: product.fact_amount,
                    PRICE: product.price,
                };
            });

            if (await dealsService.updateDealProductRows(dealId, productRows)) {
                logAccess(
                    BASE_URL + "update_deal/",
                    `Deal ${dealId} product rows successfully updated in bx`
                );
            } else {
                throw new Error(
                    `Error while updating product rows for deal ${dealId} in bx`
                );
            }

            logAccess(
                BASE_URL + "set_fact_amount_of_products_in_deal/",
                `Fact amounts successfully updated for deal ${dealId}`
            );

            // Send response back to the client
            res.status(200).json({
                status: true,
                status_msg: "success",
                message: "Fact amounts successfully updated",
            });
        } catch (error) {
            logError(BASE_URL + "set_fact_amount_of_products_in_deal/", error);
            res
                .status(500)
                .json({ status: false, status_msg: "error", message: "server error" });
        }
    }
);

// Handler for intallation team members
app.post(BASE_URL + "set_deal_as_failed/", async (req, res) => {
    try {
        const initiatorName = req.body.initiator_full_name;

        const db = new Db();
        const user = await db.getUserByFullName(initiatorName);

        if (!user.department_ids.includes("27")) {
            res.status(403).json({
                status: false,
                status_msg: "access_denied",
                message: "User not allowed",
            });
        }

        const dealId = req.body.deal_id;
        const deal = (await getDealsWithProducts(user.id)).filter(
            (deal) => deal.id === dealId
        )[0];

        const updateResult = db.updateDealById(dealId, { is_failed: true });

        if (updateResult) {
            logAccess(
                BASE_URL + "update_deal/",
                `Deal ${dealId} successfully updated in db`
            );
        } else {
            throw new Error(`Error while setting deal ${dealId} as failed in db`);
        }

        const bxLinkDecrypted = await decryptText(process.env.BX_LINK);
        const dealsService = new DealsService(bxLinkDecrypted);
        if (await dealsService.updateDeal(dealId, { UF_CRM_1732283590096: 1 })) {
            logAccess(
                BASE_URL + "update_deal/",
                `Deal ${dealId} successfully updated in bx. Deal set as failed`
            );
        }
        const productRows = deal.products.map((product) => {
            return {
                PRODUCT_ID: product.id,
                QUANTITY: product.given_amount,
                PRICE: product.price,
            };
        });

        if (await dealsService.updateDealProductRows(dealId, productRows)) {
            logAccess(
                BASE_URL + "update_deal/",
                `Deal ${dealId} product rows successfully updated in bx`
            );
        } else {
            throw new Error(
                `Error while updating product rows for deal ${dealId} in bx`
            );
        }

        res.status(200).json({
            status: true,
            status_msg: "success",
            message: "Deal and products successfully updated",
        });
    } catch (error) {
        logError(BASE_URL + "set_deal_as_failed/", error);
        res
            .status(500)
            .json({ status: false, status_msg: "error", message: "server error" });
    }
});

// Handler for warehouse manager
app.post(BASE_URL + "update_deal/", async (req, res) => {
    try {
        const dealId = req.body.deal_id;
        const products = req.body.products;
        const assignedPersonalId = req.body.assigned_id;
        // UF_CRM_1730790163295

        // console.log("Строка 270 - req.body", req.body);


        const db = new Db();
        const bxLinkDecrypted = await decryptText(process.env.BX_LINK);

        const dealsService = new DealsService(bxLinkDecrypted);

        // Update the assigned_personal_id in the deals table
        const updateResult = db.updateDealById(dealId, {
            assigned_id: assignedPersonalId,
            is_moved: true,
        });
        if (updateResult) {
            logAccess(
                BASE_URL + "update_deal/",
                `Deal ${dealId} successfully updated in db`
            );
        } else {
            throw new Error(`Error while updating deal ${dealId} in db`);
        }

        // Update the deal's assigned ID in the external service (Bitrix, etc.)
        if (
            await dealsService.updateDeal(dealId, {
                UF_CRM_1728999528: assignedPersonalId,
                UF_CRM_1730790163295: 1,
            })
        ) {
            logAccess(
                BASE_URL + "update_deal/",
                `Deal ${dealId} successfully updated in bx`
            );
        }
        // console.log("Статус сделки изменился!!");
        // Prepare the product rows for the external service
        const productRows = products.map((product) => {
            return {
                PRODUCT_ID: product.id,
                // PRODUCT_ID: product.id + 2,
                QUANTITY: product.given_amount,
                PRICE: product.price,
            };
        });
        // console.log("строка 315, productRows", productRows);
        if (await dealsService.updateDealProductRows(dealId, productRows)) {
            logAccess(
                BASE_URL + "update_deal/",
                `Deal ${dealId} product rows successfully updated in bx`
            );
        } else {
            throw new Error(
                `Error while updating product rows for deal ${dealId} in bx`
            );
        }

        // Update deals_products table in local database
        const productsUpdateResult = db.updateDealsProducts(dealId, products);
        if (productsUpdateResult) {
            logAccess(
                BASE_URL + "update_deal/",
                `Deal ${dealId} products successfully updated in db`
            );
        } else {
            throw new Error(`Error while updating products for deal ${dealId} in db`);
        }

        // Send response back to the client
        res.status(200).json({
            status: true,
            status_msg: "success",
            message: "Deal and products successfully updated",
        });
    } catch (error) {
        logError(BASE_URL + "update_deal/", error);
        res
            .status(500)
            .json({ status: false, status_msg: "error", message: "server error" });
    }
});

// Handler for warehouse manager
app.post(BASE_URL + "get_products_from_db/", async (req, res) => {
    try {
        const initiatorName = req.body.initiator_full_name;

        const db = new Db();
        const user = await db.getUserByFullName(initiatorName);

        if (!user.department_ids.includes("45")) {
            res.status(403).json({
                status: false,
                status_msg: "access_denied",
                message: "User not allowed",
            });
            return;
        }

        const products = await db.getProducts();

        res.status(200).json({
            status: true,
            status_msg: "success",
            data: { products: products },
        });
    } catch (error) {
        logError(BASE_URL + "get_products_from_db/", error);
        res
            .status(500)
            .json({ status: false, status_msg: "error", message: "server error" });
    }
});


// Handler for admin_panel
app.post(BASE_URL + "get_users_from_db/", async (req, res) => {
    try {
        const db = new Db();
        // Получаем всех пользователей
        const users = await db.getAllUsers();

        // Если пользователей нет, возвращаем ошибку
        if (users.length === 0) {
            // console.log(users);
            return res.status(404).json({
                status: false,
                status_msg: "no_users_found",
                message: "No users found in the database",
            });
        }
        res.status(200).json({
            status: true,
            status_msg: "success",
            data: { users },  // Возвращаем пользователей
        });
    } catch (error) {
        // Логируем ошибку
        logError(BASE_URL + "get_users_from_db/", error);
        res.status(500).json({
            status: false,
            status_msg: "error",
            message: "Server error",
        });
    }
});

app.post(BASE_URL + "change_deal_status_to_failed/", async (req, res) => {
    try {
        const dealId = req.body.deal_id
        // console.log(req);

        const db = new Db();
        const bxLinkDecrypted = await decryptText(process.env.BX_LINK);
        const dealService = new DealsService(bxLinkDecrypted);

        const deal = await dealService.getDealById(dealId);

        // console.log(deal);
        const updateBXDealToFailed = await dealService.updateDeal(dealId, {
            STAGE_ID: "LOSE"
        })

        const updateDeal = db.updateDealById(dealId, {
            is_failed: 1,
            is_conducted: null,
        });

        if (updateDeal) {
            logAccess(
                BASE_URL + "change_deal_status_to_failed/",
                `Deal ${dealId} successfully updated in db`
            );
        } else {
            throw new Error(`Error while updating deal ${dealId} in db`);
        }

        res.status(200).json({
            status: true,
            status_msg: "success",
            // data: { users },  // Возвращаем пользователей
        });
    } catch (error) {
        // Логируем ошибку
        logError(BASE_URL + "change_deal_status_to_failed/", error);
        res.status(500).json({
            status: false,
            status_msg: "error",
            message: "Server error",
        });
    }
});

app.post(BASE_URL + "complete_task/", async (req, res) => {
    try {
        const dealId = req.body.deal_id;
        const bxLinkDecrypted = await decryptText(process.env.BX_LINK);
        const dealsService = new DealsService(bxLinkDecrypted);
        // console.log("dealId", dealId);

        if (
            await dealsService.completeMontajnikTask(dealId)
        ) {
            logAccess(
                BASE_URL + "complete_task/",
                `Deal ${dealId} successfully updated (added picture to task comment) in bx`
            );
        }

        res.status(200).json({
            status: true,
            status_msg: "success",
            message: "Deal and task successfully updated",
        });
    } catch (error) {
        logError(BASE_URL + "complete_task/", error);
        res
            .status(500)
            .json({ status: false, status_msg: "error", message: "server error" });
    }
});

// Handler for admin_panel
app.post(BASE_URL + "add_user_to_db/", async (req, res) => {
    try {
        const db = new Db();
        // Создаем нового пользователя
        // console.log(req.body.newUser);

        const newUser = await db.insertOneUserInDb(req.body.newUser);

        // Если пользователей нет, возвращаем ошибку
        if (!newUser) {
            // console.log(user);
            return res.status(404).json({
                status: false,
                status_msg: "no_user_data_found",
                message: "No user data found in the database",
            });
        }
        res.status(200).json({
            status: true,
            status_msg: "success",
            // data: { users },  // Возвращаем пользователей
        });
    } catch (error) {
        // Логируем ошибку
        logError(BASE_URL + "add_user_to_db/", error);
        res.status(500).json({
            status: false,
            status_msg: "error",
            message: "Server error",
        });
    }
});

// Handler for admin_panel
app.post(BASE_URL + "delete_user_from_db/", async (req, res) => {
    try {
        const db = new Db();
        // Создаем нового пользователя
        // console.log(req.body.id);

        const deletedUser = await db.deleteUserById(req.body.id);

        // Если пользователей нет, возвращаем ошибку
        if (!deletedUser) {
            return res.status(404).json({
                status: false,
                status_msg: "no_user_was_deleted",
                message: "No user was deleted in the database",
            });
        }
        res.status(200).json({
            status: true,
            status_msg: "success",
            // data: { users },  // Возвращаем пользователей
        });
    } catch (error) {
        // Логируем ошибку
        logError(BASE_URL + "delete_user_from_db/", error);
        res.status(500).json({
            status: false,
            status_msg: "error",
            message: "Server error",
        });
    }
});

// Handler for warehouse manager
app.post(
    BASE_URL + "get_info_for_warehouse_manager_fill_data_panel/",
    async (req, res) => {
        try {
            const initiatorName = req.body.initiator_full_name;

            const db = new Db();
            const user = await db.getUserByFullName(initiatorName);

            if (!user.department_ids.includes("45")) {
                res.status(403).json({
                    status: false,
                    status_msg: "access_denied",
                    message: "User not allowed",
                });
                return;
            }
            // console.log(await db.getInstallationDepartmentMembers());

            const installationDepartmentMemebers = (
                await db.getInstallationDepartmentMembers()
            ).filter(
                (member) =>
                    member.city.toLowerCase().trim() === user.city.toLowerCase().trim()
            );

            const allDeals = (await getDealsWithProducts())
                .filter(
                    (deal) =>
                        !deal.is_moved && !deal.is_approved && !deal.is_amount_missmatch
                )
                .filter(
                    (deal) =>
                        deal.city.toLowerCase().trim() === user.city.toLowerCase().trim()
                );

            res.status(200).json({
                status: true,
                status_msg: "success",
                data: {
                    installation_department_memebers: installationDepartmentMemebers,
                    all_deals: allDeals,
                },
            });
        } catch (error) {
            logError(
                BASE_URL + "get_info_for_warehouse_manager_fill_data_panel/",
                error
            );
            res
                .status(500)
                .json({ status: false, status_msg: "error", message: "server error" });
        }
    }
);

app.post(
    BASE_URL + "get_info_for_warehouse_manager_watch_data_panel/",
    async (req, res) => {
        try {
            const initiatorName = req.body.initiator_full_name;

            const db = new Db();
            const user = await db.getUserByFullName(initiatorName);
            // console.log(user);

            if (!user.department_ids.includes("45")) {
                res.status(403).json({
                    status: false,
                    status_msg: "access_denied",
                    message: "User not allowed",
                });
                return;
            }

            const installationDepartmentMemebers = (
                await db.getInstallationDepartmentMembers()
            ).filter(
                (member) =>
                    member.city.toLowerCase().trim() === user.city.toLowerCase().trim()
            );
            // console.log("123123", await getDealsWithProducts());
            const allDeals = (await getDealsWithProducts())
                .filter(
                    (deal) =>
                        deal.is_moved &&
                        !deal.is_approved &&
                        !deal.is_amount_missmatch &&
                        // !deal.is_conducted
                        !deal.is_failed
                )
                .filter(
                    (deal) =>
                        deal.city.toLowerCase().trim() === user.city.toLowerCase().trim()
                );
            // console.log("3");
            res.status(200).json({
                status: true,
                status_msg: "success",
                data: {
                    installation_department_memebers: installationDepartmentMemebers,
                    all_deals: allDeals,
                },
            });
        } catch (error) {
            logError(
                BASE_URL + "get_info_for_warehouse_manager_watch_data_panel/",
                error
            );
            res
                .status(500)
                .json({ status: false, status_msg: "error", message: "server error" });
        }
    }
);

app.post(BASE_URL + "approve_deal/", async (req, res) => {
    try {
        const initiatorName = req.body.initiator_full_name;

        const db = new Db();
        const user = await db.getUserByFullName(initiatorName);

        if (!user.department_ids.includes("45")) {
            res.status(403).json({
                status: false,
                status_msg: "access_denied",
                message: "User not allowed",
            });
            return;
        }

        const dealId = req.body.deal_id;

        db.updateDealById(dealId, { is_approved: true });
        res.status(200).json({
            status: true,
            status_msg: "success",
            message: "Product rows successfully updated in BX",
        });
    } catch (error) {
        logError(BASE_URL + "approve_deal/", error);
        res
            .status(500)
            .json({ status: false, status_msg: "error", message: "server error" });
    }
});

app.post(BASE_URL + "login/", async (req, res) => {
    try {
        const name = req.body.name;
        const lastName = req.body.last_name;
        const password = req.body.password;

        const fullName = name + " " + lastName;
        const userFromDb = await db.getUserByFullName(fullName);

        if (!userFromDb) {
            throw new Error(`No user ${fullName} in db`);
        }

        if (!userFromDb.password) {
            throw new Error(`no_pwd`);
        }

        if (
            userFromDb.name === name &&
            userFromDb.last_name === lastName &&
            userFromDb.password === password
        ) {
            logAccess(BASE_URL + "register/", `User ${name} ${lastName} logged in`);
            res.status(200).json({
                status: true,
                status_msg: "success",
                message: "User logged in",
                user_data: { name: name, last_name: lastName },
            });
        } else {
            logAccess(
                BASE_URL + "register/",
                `Invalid credentials for user ${name} ${lastName}`
            );
            res.status(400).json({
                status: false,
                status_msg: "error",
                message: "Invalid credentials for user",
            });
        }
    } catch (error) {
        logError(BASE_URL + "register/", error);
        res
            .status(500)
            .json({ status: false, status_msg: "error", message: error.message });
    }
});

app.post(BASE_URL + "get_user_from_bx/", async (req, res) => {
    try {
        const filter = req.body.filter;
        // console.log("filter", filter);

        const bxLink = await decryptText(process.env.BX_LINK);
        const usersService = new UsersService(bxLink);
        const user = await usersService.getUserByFilter(filter)

        if(user) {
            logAccess(
                BASE_URL + "get_user_from_bx/",
                `User successfully found from bx`
            );
            res.status(200).json({
                status: true,
                user: user.result,
                status_msg: "success",
                message: "User successfully found from bx",
            });
        } else {
            logError(BASE_URL + "get_user_from_bx/", error);
        }

    } catch (error) {
        logError(BASE_URL + "get_user_from_bx/", error);
        res
            .status(500)
            .json({ status: false, status_msg: "error", message: "server error" });
    }
});

app.post(BASE_URL + "register/", async (req, res) => {
    try {
        const name = req.body.name;
        const lastName = req.body.last_name;
        const password = req.body.password;

        const bxLinkDecrypted = await decryptText(process.env.BX_LINK);

        const db = new Db();
        const usersService = new UsersService(bxLinkDecrypted);

        const user = (
            await usersService.getUsersListByFilter({
                NAME: name,
                LAST_NAME: lastName,
            })
        ).map((user) => {
            return {
                id: user["ID"],
                name: user["NAME"],
                last_name: user["LAST_NAME"],
                departments: user["UF_DEPARTMENT"],
                password: password,
            };
        })[0];
        if (user) {
            const insertResult = db.updateUserInDb(user.id, user);
            if (insertResult) {
                logAccess(
                    BASE_URL + "register/",
                    `User ${user.id} ${user.name} ${user.last_name} successfully added to db`
                );
                res.status(200).json({
                    status: true,
                    status_msg: "success",
                    message: `User ${user.id} ${user.name} ${user.last_name} successfully added to db`,
                });
            } else {
                throw new Error("Error while adding user to db");
            }
        } else {
            logError(BASE_URL + "register/", `User ${name} ${lastName} not found`);
            res.status(400).json({
                status: false,
                status_msg: "error",
                message: `User ${name} ${lastName} not found`,
            });
        }
    } catch (error) {
        logError(BASE_URL + "register/", error);
        res
            .status(500)
            .json({ status: false, status_msg: "error", message: "server error" });
    }
});

app.post(BASE_URL + "check_user_permission/", async (req, res) => {
    try {
        const name = req.body.name;
        const lastName = req.body.last_name;

        const db = new Db();
        const fullName = name + " " + lastName;
        const userFromDb = await db.getUserByFullName(fullName);

        if (!userFromDb) {
            throw new Error(`No user ${fullName} in db`);
        }

        const userWarehouseManagerDeaprtmentId =
            userFromDb.department_ids.split(",").find((dep) => Number(dep) === 45) ||
            null;
        const userAdminPanelDeaprtmentId =
            userFromDb.department_ids.split(",").find((dep) => Number(dep) === 53) ||
            null;
        const userInstallationTeamDeaprtmentId =
            userFromDb.department_ids.split(",").find((dep) => Number(dep) === 27) ||
            null;
        if (userWarehouseManagerDeaprtmentId && userInstallationTeamDeaprtmentId && userAdminPanelDeaprtmentId) {
            res.status(200).json({
                status: true,
                status_msg: "success",
                message: "User has permissions",
                permissions: ["warehouse_manager", "installation_team", "admin_panel"],
            });
        } else if (userWarehouseManagerDeaprtmentId) {
            res.status(200).json({
                status: true,
                status_msg: "success",
                message: "User has permissions",
                permissions: ["warehouse_manager"],
            });
        } else if (userInstallationTeamDeaprtmentId) {
            res.status(200).json({
                status: true,
                status_msg: "success",
                message: "User has permissions",
                permissions: ["installation_team"],
            });
        } else if (userAdminPanelDeaprtmentId) {
            res.status(200).json({
                status: true,
                status_msg: "success",
                message: "User has permissions",
                permissions: ["admin_panel"],
            });
        } else {
            throw new Error(`User ${fullName} doesn't have permission`);
        }
    } catch (error) {
        logError(BASE_URL + "check_user_permission/", error);
        res
            .status(500)
            .json({ status: false, status_msg: "error", message: "server error" });
    }
});

app.post(BASE_URL + "add_deal_handler/", async (req, res) => {
    try {
        const dealId = req.body["data[FIELDS][ID]"];
        const bxLinkDecrypted = await decryptText(process.env.BX_LINK);

        const db = new Db();
        const dealService = new DealsService(bxLinkDecrypted);
        const productService = new ProductsService(bxLinkDecrypted);

        const newDeal = [await dealService.getDealById(dealId)]
            .map((deal) => {
                // console.log("Строка 930, deal -", deal);

                if (Number(deal["CATEGORY_ID"]) === 0) {
                    return {
                        id: deal["ID"],
                        title: deal["TITLE"],
                        date_create: deal["UF_CRM_1728999194580"],
                        assigned_id: deal["UF_CRM_1728999528"] || null,
                        city: deal["UF_CRM_1732081124429"] || null,
                        service_price: deal["UF_CRM_1732531742220"] || null,
                    };
                }
            })
            .filter((deal) => deal !== undefined);

        let insertResult = await db.insertDealsInDb(newDeal);
        if (insertResult) {
            logAccess(
                BASE_URL + "add_deal_handler",
                `Deal ${dealId} successfully added to db`
            );
        } else {
            throw new Error(`Error while deal ${dealId} in db`);
        }

        const productsForProductsTable = [];

        const productRows = (
            await dealService.getDealProductRowsByDealId(dealId)
        ).map((pr) => {
            // console.log("Строка 823 - Название товара -", pr["PRODUCT_NAME"]);
            // console.log("Строка 824 - ID товара -", Number(pr["PRODUCT_ID"]));
            productsForProductsTable.push({name: pr["PRODUCT_NAME"]})
            // console.log("Строка 826 - Массив с товарами (иттерация) -", productsForProductsTable);
            return {
                product_name: pr["PRODUCT_NAME"],
                product_id: Number(pr["PRODUCT_ID"]),
                given_amount: Number(pr["QUANTITY"]),
                price: Number(pr["PRICE"]),
            };
        });

        // console.log("Строка 835 - Массив с товарами -", productsForProductsTable);

        const products = [];
        let counter = 0;
        for (let pr of productRows) {
            // console.log("Строка 837 -", pr.product_id);
            const originalProduct = await productService.getOriginalProductId(
                pr.product_id
            );
            // console.log("originalProduct", originalProduct);
            if (originalProduct && Object.keys(originalProduct).length > 0) {
                productsForProductsTable[counter].id = originalProduct.parentId.value;
                products.push({
                    deal_id: dealId,
                    product_id: originalProduct.parentId.value,
                    given_amount: pr.given_amount,
                    fact_amount: null,
                    price: pr.price,
                });
            } else {
                productsForProductsTable[counter].id = pr.product_id;
                products.push({
                    deal_id: dealId,
                    product_id: pr.product_id,
                    given_amount: pr.given_amount,
                    fact_amount: null,
                    price: pr.price,
                });
            }
            counter++
        }
        const dealProducts = products.map((pr) => {
            return {
                deal_id: dealId,
                product_id: pr.product_id,
                given_amount: pr.given_amount,
                fact_amount: null,
                price: pr.price,
            };
        });
        // console.log("productsForProductsTable - ", productsForProductsTable);

        // console.log("dealProducts - ", dealProducts);

        insertProductsResults = await db.insertProductsInDb(productsForProductsTable)
        insertResult = await db.insertDealsProductsInDb(dealProducts);
        if (insertResult) {
            logAccess(
                BASE_URL + "add_deal_handler/",
                `Product rows for deal ${dealId} successfully added to db`
            );
        } else {
            throw new Error(
                `Error while adding product rows for deal ${dealId} in db`
            );
        }
    } catch (error) {
        logError(BASE_URL + "add_deal_handler/", error);
        res
            .status(500)
            .json({ status: false, status_msg: "error", message: "server error" });
    }
});

app.post(BASE_URL + "add_product_handler/", async (req, res) => {
    try {
        const productId = req.body["data[FIELDS][ID]"];

        const bxLinkDecrypted = await decryptText(process.env.BX_LINK);

        const db = new Db();
        const productsService = new ProductsService(bxLinkDecrypted);

        const newProduct = (await productsService.getProductById(productId)).map(
            (product) => {
                return {
                    id: product["ID"],
                    name: product["NAME"],
                };
            }
        );

        const insertResult = db.insertProductsInDb(newProduct);
        if (insertResult) {
            logAccess(
                BASE_URL + "add_product_handler/",
                `Product ${productId} successfully added to db`
            );
            res.status(200).json({
                status: true,
                status_msg: "success",
                message: `Product ${productId} successfully added to db`,
            });
        } else {
            throw new Error(`Error while adding product ${productId} in db`);
        }
    } catch (error) {
        logError(BASE_URL + "add_deal_handler/", error);
        res
            .status(500)
            .json({ status: false, status_msg: "error", message: "server error" });
    }
});

app.post(BASE_URL + "get_from_bx_insert_users_in_db/", async (req, res) => {
    try {
        const filter = req.body.filter;

        const bxLink = await decryptText(process.env.BX_LINK);

        const db = new Db();

        const usersService = new UsersService(bxLink);
        const users = (await usersService.getUsersListByFilter(filter)).map(
            (user) => {
                return {
                    id: user["ID"],
                    name: user["NAME"],
                    last_name: user["LAST_NAME"],
                    departments: user["UF_DEPARTMENT"],
                };
            }
        );

        const insertResult = db.insertUsersInDb(users);
        if (insertResult) {
            logAccess(
                BASE_URL + "get_from_bx_insert_users_in_db/",
                "Users successfully added to db"
            );
            res.status(200).json({
                status: true,
                status_msg: "success",
                message: "Users successfully added to db",
            });
        } else {
            throw new Error("Error while inserting users in db");
        }
    } catch (error) {
        logError(BASE_URL + "get_from_bx_insert_users_in_db/", error);
        res
            .status(500)
            .json({ status: false, status_msg: "error", message: "server error" });
    }
});

app.post(BASE_URL + "get_from_bx_insert_products_in_db/", async (req, res) => {
    try {
        const db = new Db();
        const bxLinkDecrypted = await decryptText(process.env.BX_LINK);

        const productsService = new ProductsService(bxLinkDecrypted);
        const products = (await productsService.getProductList()).map((product) => {
            return {
                id: product["ID"],
                name: product["NAME"],
            };
        });

        const insertResult = db.insertProductsInDb(products);

        if (insertResult) {
            logAccess(
                BASE_URL + "get_from_bx_insert_products_in_db/",
                "Products successfully added to db"
            );
            res.status(200).json({
                status: true,
                status_msg: "success",
                message: "Products successfully added to db",
            });
        } else {
            throw new Error("Error while adding products in db");
        }
    } catch (error) {
        logError(BASE_URL + "get_from_bx_insert_products_in_db/", error);
        res
            .status(500)
            .json({ status: false, status_msg: "error", message: "server error" });
    }
});

app.post(BASE_URL + "get_from_bx_insert_deals_in_db/", async (req, res) => {
    try {
        const filter = req.body.filter || {};

        const bxLink = await decryptText(process.env.BX_LINK);

        const db = new Db();

        filter[">ID"] = (await db.getDealMaxId()).max_id;

        const dealsService = new DealsService(bxLink);
        const deals = (await dealsService.getDealsListByFilter(filter))
            .map((deal) => {
                if (Number(deal["CATEGORY_ID"]) === 0) {
                    return {
                        id: deal["ID"],
                        title: deal["TITLE"],
                        date_create: deal["UF_CRM_1728999194580"],
                        assigned_id: deal["UF_CRM_1728999528"] || null,
                        city: deal["UF_CRM_1732081124429"] || null,
                        service_price: deal["UF_CRM_1732531742220"] || null,
                    };
                }
            })
            .filter((deal) => deal !== undefined);
        const insertResult = db.insertDealsInDb(deals);
        if (insertResult) {
            logAccess(
                BASE_URL + "get_from_bx_insert_deals_in_db/",
                "Deals successfully added to db"
            );
            res.status(200).json({
                status: true,
                status_msg: "success",
                message: "Deals successfully added to db",
            });
        } else {
            throw new Error("Error while adding deals in db");
        }
    } catch (error) {
        logError(BASE_URL + "get_from_bx_insert_deals_in_db/", error);
        res
            .status(500)
            .json({ status: false, status_msg: "error", message: "server error" });
    }
});

app.post(
    BASE_URL + "get_from_bx_insert_deals_products_in_db/",
    async (req, res) => {
        try {
            const db = new Db();
            const dealsFromDb = await db.getDeals();

            const bxLinkDecrypted = await decryptText(process.env.BX_LINK);
            const dealsService = new DealsService(bxLinkDecrypted);
            const productService = new ProductsService(bxLinkDecrypted);

            let dealProducts = [];
            for (const deal of dealsFromDb) {
                const productRows = (
                    await dealsService.getDealProductRowsByDealId(deal.id)
                ).map((pr) => {
                    return {
                        product_id: Number(pr["PRODUCT_ID"]),
                        given_amount: Number(pr["QUANTITY"]),
                    };
                });
                for (let pr of productRows) {
                    const originalProduct = await productService.getOriginalProductId(
                        pr.product_id
                    );
                    if (Object.keys(originalProduct).length > 0) {
                        dealProducts.push({
                            deal_id: deal.id,
                            product_id: originalProduct.parentId.value,
                            given_amount: pr.given_amount,
                            fact_amount: null,
                        });
                    } else {
                        dealProducts.push({
                            deal_id: deal.id,
                            product_id: pr.product_id,
                            given_amount: pr.given_amount,
                            fact_amount: null,
                        });
                    }
                }
            }

            const insertResult = db.insertDealsProductsInDb(dealProducts);
            if (insertResult) {
                logAccess(
                    BASE_URL + "get_from_bx_insert_deals_products_in_db/",
                    "DealProducts successfully added to db"
                );
                res.status(200).json({
                    status: true,
                    status_msg: "success",
                    message: "DealProducts successfully added to db",
                });
            } else {
                throw new Error("Error while adding deals in db");
            }
        } catch (error) {
            logError(BASE_URL + "get_from_bx_insert_deals_products_in_db/", error);
            res
                .status(500)
                .json({ status: false, status_msg: "error", message: "server error" });
        }
    }
);

app.post(BASE_URL + "delete_deal_handler", async (req, res) => {
    try {
        let id = req.query["ID"];
        if (!id) {
            id = req.body["data[FIELDS][ID]"];
        }
        if (!id) {
            logError(BASE_URL + "delete_deal_handler", "No deal id provided");
            res.status(400).json({
                status: false,
                status_msg: "error",
                message: "No deal id provided",
            });
            return;
        }

        const db = new Db();
        const deleteDealResult = db.deleteDealById(id);
        if (deleteDealResult) {
            const deleteDealProductsResult = db.deleteDealsProductsRowByDealId(id);
            if (deleteDealProductsResult) {
                logAccess(
                    BASE_URL + "delete_deal_handler",
                    `Deal ${id} successfully deleted`
                );
                res.status(200).json({
                    status: true,
                    status_msg: "success",
                    message: `Deal ${id} successfully deleted`,
                });
            }
        }
    } catch (error) {
        logError(BASE_URL + "delete_deal_handler", error);
        res
            .status(500)
            .json({ status: false, status_msg: "error", message: "server  error" });
    }
});

app.post(BASE_URL + "delete_deal", async (req, res) => {
    try {
        let id = 0;
        // console.log(req.body.deal_id);

        id = req.body.deal_id;
        // console.log(id);

        if (!id) {
            logError(BASE_URL + "delete_deal", "No deal id provided");
            res.status(400).json({
                status: false,
                status_msg: "error",
                message: "No deal id provided",
            });
            return;
        }

        // console.log(id);

        const db = new Db();
        const deleteDealResult = db.deleteDealById(id);
        if (deleteDealResult) {
            const deleteDealProductsResult = db.deleteDealsProductsRowByDealId(id);
            if (deleteDealProductsResult) {
                logAccess(
                    BASE_URL + "delete_deal",
                    `Deal ${id} successfully deleted`
                );
                res.status(200).json({
                    status: true,
                    status_msg: "success",
                    message: `Deal ${id} successfully deleted`,
                });
            }
        }
    } catch (error) {
        logError(BASE_URL + "delete_deal", error);
        res
            .status(500)
            .json({ status: false, status_msg: "error", message: "server  error" });
    }
});

app.post(BASE_URL + "update_deal_handler/", async (req, res) => {
    try {
        let id = req.query["ID"];
        if (!id) {
            id = req.body["data[FIELDS][ID]"];
        }
        if (!id) {
            logError(BASE_URL + "update_deal_handler", "No deal id provided");
            res.status(400).json({
                status: false,
                status_msg: "error",
                message: "No deal id provided",
            });
            return;
        }

        const db = new Db();
        const bxLinkDecrypted = await decryptText(process.env.BX_LINK);
        const dealService = new DealsService(bxLinkDecrypted);
        const productService = new ProductsService(bxLinkDecrypted);

        const updatedDeal = [await dealService.getDealById(id)].map((deal) => {
            return {
                id: deal["ID"],
                title: deal["TITLE"],
                date_create: deal["UF_CRM_1728999194580"],
                assigned_id: deal["UF_CRM_1728999528"] || null,
                city: deal["UF_CRM_1732081124429"] || null,
                service_price: deal["UF_CRM_1732531742220"] || null,
            };
        });
        const updateResult = await db.updateDealById(id, updatedDeal[0]);
        if (updateResult) {
            logAccess(
                BASE_URL + "update_deal_handler/",
                `Deal ${id} successfully updated in db`
            );
        } else {
            throw new Error(`Error while setting deal ${id} as failed in db`);
        }
        const dealsProductsFromDb = await db.getDealsProducts(id);
        // console.log("Строка 1357, Материалы из сделки:", dealsProductsFromDb);

        // запрос в базу данных чтобы доставать товары из сделки (таблица deals_products)
        const productRows = (await dealService.getDealProductRowsByDealId(id)).map((pr) => {
            // if (Number(pr["QUANTITY"]) !== 0) {
            // Ищем соответствующий продукт в dealsProductsFromDb по product_id
            const matchedProduct = dealsProductsFromDb.find(
                (dp) => dp.product_id === Number(pr["PRODUCT_ID"])
            );
            // console.log("Строка 1401, matchedProduct - ", matchedProduct);

            return {
                product_id: Number(pr["PRODUCT_ID"]),
                // Если продукт найден, берем fact_amount из базы, иначе null
                fact_amount: matchedProduct ? Number(pr["QUANTITY"]) : 0,
                // Если продукт найден, берем given_amount из базы, иначе из pr["QUANTITY"]
                given_amount: matchedProduct
                    ? matchedProduct.given_amount
                    : Number(pr["QUANTITY"]),
                price: Number(pr["PRICE"]),
            };
            // }
        });
        // const productRows = (await dealService.getDealProductRowsByDealId(id)).map(
        //   (pr, index) => {
        //     // console.log("Строка 1362, Строки:", pr);
        //     // console.log(index);

        //     if (Number(pr["QUANTITY"]) !== 0) {
        //       return {
        //         product_id: Number(pr["PRODUCT_ID"]),
        //         fact_amount: dealsProductsFromDb[index] ? dealsProductsFromDb[index].fact_amount : null, // брать given_amount из базы данных
        //         given_amount: dealsProductsFromDb[index] ? dealsProductsFromDb[index].given_amount : Number(pr["QUANTITY"]),
        //         price: Number(pr["PRICE"]),
        //       };
        //     }
        //   }
        // );
        // console.log("Строка 1375, productRows:", productRows);
        // console.log("Строка 1376, dealService:", dealService);

        const products = [];
        // console.log("Строка 1471, productRows - ", productRows);
        for (let pr of productRows) {
            // console.log("Строка 1473, pr - ", pr);

            if(pr.product_id) {
                const originalProduct = await productService.getOriginalProductId(
                    pr.product_id
                );
                // console.log("строка 1383", originalProduct);

                if (originalProduct && Object.keys(originalProduct).length > 0) {
                    products.push({
                        deal_id: id,
                        product_id: originalProduct.parentId.value,
                        given_amount: pr.given_amount,
                        // fact_amount: null,
                        fact_amount: pr.fact_amount,
                        price: pr.price,
                    });
                } else {
                    products.push({
                        deal_id: id,
                        product_id: pr.product_id,
                        given_amount: pr.given_amount,
                        // fact_amount: null,
                        fact_amount: pr.fact_amount,
                        price: pr.price,
                    });
                }
            } else {
                throw new Error(`Error while updating product rows for deal ${id} in db`);
            }
        }
        // console.log("Строка 1405", products);

        const dealProducts = products.map((pr) => {
            return {
                deal_id: id,
                product_id: pr.product_id,
                given_amount: pr.given_amount,
                // fact_amount: null,
                fact_amount: pr.fact_amount,
                price: pr.price,
            };
        });
        // console.log("Строка 1417", dealProducts);


        const insertResult = db.insertDealsProductsInDb(dealProducts);
        if (insertResult) {
            logAccess(
                BASE_URL + "update_deal_handler/",
                `Product rows for deal ${id} successfully updated in db`
            );
        } else {
            throw new Error(`Error while updating product rows for deal ${id} in db`);
        }

        res.status(200).json({
            status: true,
            status_msg: "success",
            message: `Сделка ${id} успешно обновлена`,
        });
    } catch (error) {
        logError(BASE_URL + "update_deal_handler", error);
        res
            .status(500)
            .json({ status: false, status_msg: "error", message: "server  error" });
    }
});

app.post(BASE_URL + "deny_deal/", async (req, res) => {
    try {
        const dealId = req.body.deal_id;

        const db = new Db();
        const bxLinkDecrypted = await decryptText(process.env.BX_LINK);

        const dealsService = new DealsService(bxLinkDecrypted);

        // Update the assigned_personal_id in the deals table
        const updateResult = db.updateDealById(dealId, { is_amount_missmatch: 1 });
        if (updateResult) {
            logAccess(
                BASE_URL + "deny_deal/",
                `Deal ${dealId} successfully updated in db`
            );
        } else {
            throw new Error(`Error while updating deal ${dealId} in db`);
        }

        // Update the deal's assigned ID in the external service (Bitrix, etc.)
        if (await dealsService.updateDeal(dealId, { UF_CRM_1732524504063: 1 })) {
            logAccess(
                BASE_URL + "deny_deal/",
                `Deal ${dealId} successfully updated in bx`
            );
        }

        // Send response back to the client
        res.status(200).json({
            status: true,
            status_msg: "success",
            message: "Deal and products successfully updated",
        });
    } catch (error) {
        logError(BASE_URL + "deny_deal/", error);
        res
            .status(500)
            .json({ status: false, status_msg: "error", message: "server error" });
    }
});

app.post(BASE_URL + "tmp/", async (req, res) => {
    res.status(200).json({});
});

async function getDealsWithProducts(assigned_id = null) {
    const allDeals = await db.getDeals(assigned_id);
    // console.log("12");
    // console.log(allDeals);

    const allProducts = await db.getProducts();
    // console.log("23");
    const dealProducts = await db.getDealsProducts();
    // console.log("34");
    const dealsWithProducts = allDeals.map((deal) => {
        // Find products associated with the current deal by matching IDs
        const productsInDeal = dealProducts
            .filter((dp) => dp.deal_id === deal.id)
            .map((dp) => {
                const product = allProducts.find((p) => p.id === dp.product_id);
                if (!product) {
                    logError(
                        "getDealsWithProducts",
                        `Product with ID ${dp.product_id} not found. Deal - ${deal.id}`
                    );
                    return null; // Skip or handle as necessary
                }
                return {
                    id: product.id,
                    name: product.name,
                    given_amount: dp.given_amount,
                    fact_amount: dp.fact_amount,
                    total: dp.total,
                    price: dp.price,
                };
            })
            .filter((product) => product !== null); // Remove null entries

        return { ...deal, products: productsInDeal };
    });
    // console.log("12123123123123", dealsWithProducts);

    return dealsWithProducts;
}

app.listen(PORT, () => {
    console.log(`Server is running on port: ${PORT}`);
});
