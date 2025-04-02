const {Bitrix} = require("@2bad/bitrix")
const {logError, logAccess} = require("../logger/logger");

const pageSize = 50;

class DealsService {
    bx;

    constructor(link) {
        this.bx = Bitrix(link);
        this.bx_link = link
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
            } while (start < total)
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
            } while (start < total)

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

    async updateDealWithPicture(dealId, updatingFieldCom = {}, updatingFieldPic) {
        try {
            // console.log(updatingFieldPic);
            // console.log(updatingFieldPic.UF_CRM_1740324915.length);
            await this.bx.call("crm.deal.update", {
                id: dealId,
                fields: updatingFieldCom
            });
            // console.log("this.bx_link", this.bx_link);
            // console.log(this.bx_link + "crm.deal.update");

            if (updatingFieldPic.UF_CRM_1740324915.length !== 0) {
                const response = await fetch(this.bx_link + "crm.deal.update", {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                    },
                    body: JSON.stringify({
                        id: dealId,
                        fields: updatingFieldPic,
                    })
                })

                if (response.ok) {
                    console.log("Deal edited");
                }
            }
            return true;
        } catch (error) {
            logError("DealsService updateDeal", error);
            return false;
        }
    }

    async addPictureToTaskComment(dealId, pictureData = {},) {
        try {
            const actualId = `D_${dealId}`

            const dealTasks = await this.bx.call("tasks.task.list",
                {filter: {"UF_CRM_TASK": actualId}},
            );

            const actualTask = dealTasks.result.tasks.find(obj => obj.title.includes("Произведение работ"));

            if (!actualTask) {
                console.log(`No task found for dealId: ${dealId}`);
                return false;
            }

            await this.bx.call("task.commentitem.add",
                {
                    TASKID: actualTask.id,
                    FIELDS: {
                        POST_MESSAGE: pictureData.comment,
                        AUTHOR_ID: pictureData.userID,
                    }
                }
            );
            return true;
        } catch (error) {
            logError("DealsService addPictureToTaskComment", error);
            return false;
        }
    }

    async completeMontajnikTask(dealId) {
        try {

            const actualId = `D_${dealId}`
            // console.log("actualId", actualId);
            const dealTasks = await this.bx.call("tasks.task.list",
                {filter: {"UF_CRM_TASK": actualId}},
            );

            const actualTask = dealTasks.result.tasks.find(obj => obj.title.includes("Произведение работ"));

            // console.log("actualTask", actualTask);

            if (!actualTask) {
                console.log(`No task found for dealId: ${dealId}`);
                return false;
            }

            await this.bx.call('tasks.task.complete',
                {
                    taskId: actualTask.id,
                }
            );
            return true;
        } catch (error) {
            logError("DealsService completeMontajnikTask", error);
            return false;
        }
    }

    async updateDealProductRows(dealId, productRows = []) {
        // console.log("Другой файл, строка 192, dealId", dealId);
        // console.log("Другой файл, строка 193, productRows", productRows);

        try {
            const res = await this.bx.call("crm.deal.productrows.set", {
                id: dealId,
                rows: productRows
            });
            return true;
        } catch (error) {
            console.error('Bitrix Response:', error.response?.body); // Добавьте эту строку
            logError("DealsService updateDeal", error);
            return false;
        }
    }

    async uploadPictureToDealUserField(dealId, base64Image) {
        try {
            // Извлекаем mime-тип и чистую base64-строку
            const matches = base64Image.match(/^data:image\/([a-zA-Z]+);base64,(.+)$/);
            if (!matches || matches.length !== 3) {
                throw new Error('Неверный формат base64');
            }

            const fileType = matches[1]; // Например, "png", "jpeg"
            const fileContent = matches[2]; // Чистая base64-строка без префикса

            // Формируем имя файла
            const fileName = `image_${Date.now()}.${fileType}`;

            // Подготавливаем данные для пользовательского поля
            const fileData = {
                "fileData": [fileName, fileContent]
            };

            // Отправляем запрос через fetch с использованием POST
            const response = await fetch(this.bx_link + "crm.deal.update", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({
                    id: dealId,
                    fields: {
                        "UF_CRM_1743592211012": fileData // Поле для изображения
                    }
                })
            });

            // Проверяем успешность запроса
            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(`Ошибка API: ${errorData.error_description || response.statusText}`);
            }

            return true;
        } catch (error) {
            logError("DealsService uploadPictureToDealUserField", error);
            return false;
        }
    }
}

module.exports = DealsService;