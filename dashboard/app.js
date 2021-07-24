// noinspection JSValidateTypes

const defaultTo = require("lodash/defaultTo");
const kafka = require("kafka-node");
const app = require("express")();
const server = require("http").createServer(app);
const io = require("socket.io")(server, {
    cors: {origin: "*"},
});

const PORT = 5005;
const client = new kafka.KafkaClient({kafkaHost: "localhost:9092"});
const consumer = new kafka.Consumer(client, [{topic: "access-logs-sink"}], {
    autoCommit: false,
});

const toSchema = (obj) => {
    if ("browser_categories" in obj) {
        return {
            key: "browser_categories",
            data: {
                [obj.browser_categories.accessed_by]: obj.browser_categories.count,
            },
        };
    }
    if ("types_of_vendors" in obj) {
        return {
            key: "types_of_vendors",
            data: {
                [obj.types_of_vendors.browser_vendor]: obj.types_of_vendors.count,
            },
        };
    }
    if ("types_of_operating_systems" in obj) {
        return {
            key: "types_of_operating_systems",
            data: {
                [obj.types_of_operating_systems.os]:
                obj.types_of_operating_systems.count,
            },
        };
    }
    if ("types_of_brands" in obj) {
        return {
            key: "types_of_brands",
            data: {[obj.types_of_brands.brand]: obj.types_of_brands.count},
        };
    }
    if ("types_of_devices" in obj) {
        return {
            key: "types_of_devices",
            data: {[obj.types_of_devices.device_type]: obj.types_of_devices.count},
        };
    }
};

const database = {};

io.on("connection", (client) => {
    console.log("Client Connected", client);
    consumer.on("message", function (message) {
        const flatten = toSchema(JSON.parse(String(message.value)));
        if (flatten.key) {
            database[flatten.key] = Object.assign(
                defaultTo(database[flatten.key], {}),
                flatten.data
            );
        }
        client.emit("XYZ_DATA", database);
    });

    client.on("disconnect", () => {
        console.log("Client disconnected");
    });
});

server.listen(PORT, () => {
    console.log(`Started and listening on port ${server.address().port}`);
});
