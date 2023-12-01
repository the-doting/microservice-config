import { ServiceSchema } from "../../../lib/types";

import DBMixin from "moleculer-db";
import SqlAdapter from "moleculer-db-adapter-sequelize";
import Sequelize from "sequelize";

(DBMixin as any).actions = {};

const Service: ServiceSchema = {
	name: "config",
	version: "api.v1",

	/**
	 * Mixins
	 */
	mixins: [DBMixin],

	adapter: new SqlAdapter(process.env.DATABASE_URL || "sqlite://:memory:"),

	model: {
		name: "config",
		define: {
			key: {
				type: Sequelize.STRING,
			},
			value: {
				// save json as string
				type: Sequelize.TEXT,
			},
			createdBy: {
				type: Sequelize.STRING,
			},
		},
	},

	/**
	 * Service settings
	 */
	settings: {},

	/**
	 * Service dependencies
	 */
	// dependencies: [],

	/**
	 * Actions
	 */
	actions: {
		search: {
			permission: ["api.v1.config.search"],
			rest: "POST /search",
			params: {
				key: {
					type: "string",
					min: 3,
					optional: true,
					default: "",
				},
				page: {
					type: "number",
					optional: true,
					default: 1,
					min: 1,
					integer: true,
					positive: true,
				},
				limit: {
					type: "number",
					optional: true,
					default: 10,
					min: 1,
					integer: true,
					positive: true,
				},
				sort: {
					type: "enum",
					optional: true,
					values: ["desc:createdBy", "asc:createdBy", "desc:key", "asc:key"],
				},
			},
			async handler(ctx) {
				try {
					const createdBy = ctx.meta.creator.trim().toLowerCase();
					let { key, page, limit, sort } = ctx.params;

					key = key.trim().toUpperCase();

					const fields = ["key", "createdBy", "createdAt", "updatedAt"];

					/**
					 * Find all configs by page and limit and give total count of configs with where
					 */

					let sql = `SELECT ${fields.join(",")} FROM configs`;

					let where = [];

					if (key && key.length > 0) {
						where.push(`key LIKE '%${key}%'`);
					}

					if (createdBy && createdBy.length > 0) {
						where.push(`createdBy = '${createdBy}'`);
					}

					if (where.length > 0) {
						sql += ` WHERE ${where.join(" AND ")}`;
					}

					if (sort) {
						const [order, field] = sort.split(":");
						sql += ` ORDER BY ${field} ${order}`;
					}

					sql += ` LIMIT ${limit} OFFSET ${(page - 1) * limit}`;

					const [configs] = await this.adapter.db.query(sql);
					const [[{ count }]] = await this.adapter.db.query(
						`SELECT COUNT(*) AS count FROM configs ${
							where.length > 0 ? `WHERE ${where.join(" AND ")}` : ""
						}`
					);

					return {
						code: 200,
						i18n: "CONFIGS_FOUND",
						meta: {
							page,
							limit,
							total: count,
							last: Math.max(Math.ceil(count / limit), 1),
						},
						data: configs,
					};
				} catch (error) {
					console.error(error);
					return {
						code: 500,
						i18n: "#INTERNAL_SERVER_ERROR",
					};
				}
			},
		},
		multiplex: {
			permission: ["api.v1.config.multiplex"],
			rest: "POST /get",
			params: {
				keys: {
					type: "array",
					items: "string",
					min: 1,
				},
			},
			async handler(ctx) {
				try {
					const createdBy = ctx.meta.creator.trim().toLowerCase();
					let { keys } = ctx.params;

					keys = keys.map((key: string) => key.trim().toUpperCase());

					const [configs] = await this.adapter.db.query(
						`SELECT * FROM configs WHERE key IN ('${keys.join("','")}') ${
							createdBy.length > 0 ? `AND createdBy = '${createdBy}'` : ""
						}`
					);

					let data: any = {};

					for (const config of configs) {
						let value = config.value;

						try {
							value = JSON.parse(value);
						} catch (error) {}

						data[config.key] = {
							exists: true,
							key: config.key,
							value: value,
							createdBy: config.createdBy,
							createdAt: config.createdAt,
							updatedAt: config.updatedAt,
						};
					}

					// add not found keys
					for (const key of keys) {
						if (!data[key]) {
							data[key] = {
								exists: false,
								key,
								value: null,
							};
						}
					}

					return {
						code: 200,
						i18n: "CONFIGS_FOUND",
						data,
					};
				} catch (error) {
					return {
						code: 500,
					};
				}
			},
		},
		get: {
			permission: ["api.v1.config.get"],
			rest: "GET /get/:key",
			params: {
				key: {
					type: "string",
					min: 3,
				},
			},
			async handler(ctx) {
				try {
					const createdBy = ctx.meta.creator.trim().toLowerCase();
					let { key } = ctx.params;

					key = key.trim().toUpperCase();

					const [config] = await this.adapter.db.query(
						`SELECT * FROM configs WHERE key = '${key}' ${
							createdBy.length > 0 ? `AND createdBy = '${createdBy}'` : ""
						}`
					);

					if (config.length > 0) {
						let value = config[0].value;

						try {
							value = JSON.parse(value);
						} catch (error) {}

						return {
							code: 200,
							i18n: "CONFIG_FOUND",
							data: {
								key: config[0].key,
								value: value,
								createdBy: config[0].createdBy,
								createdAt: config[0].createdAt,
								updatedAt: config[0].updatedAt,
							},
						};
					} else {
						return {
							code: 404,
							i18n: "CONFIG_NOT_FOUND",
							data: {
								key: key,
							},
						};
					}
				} catch (error) {
					return {
						code: 500,
					};
				}
			},
		},
		set: {
			permission: ["api.v1.config.set"],
			rest: "POST /set",
			params: {
				key: {
					type: "string",
					min: 3,
				},
				value: {
					type: "any",
				},
			},
			async handler(ctx) {
				try {
					const createdBy = ctx.meta.creator.trim().toLowerCase();
					let { key, value } = ctx.params;

					key = key.trim().toUpperCase();

					// if typeof value is object, stringify it
					if (typeof value == "object") value = JSON.stringify(value);

					const [config] = await this.adapter.db.query(
						`SELECT * FROM configs WHERE key = '${key}' AND createdBy = '${createdBy}'`
					);

					if (config.length > 0) {
						value = value.replace(/'/g, "''");
						// update
						await this.adapter.db.query(
							`UPDATE configs SET value = '${value}' WHERE key = '${key}' AND createdBy = '${createdBy}'`
						);
					} else {
						await this.adapter.insert({
							key,
							value,
							createdBy,
						});
					}

					return {
						code: 200,
						i18n: "CONFIG_SET",
						data: {
							key,
							value: ctx.params.value,
							createdBy,
						},
					};
				} catch (error) {
					console.error(error);
					return {
						code: 500,
					};
				}
			},
		},
		bulk: {
			permission: ["api.v1.config.bulk"],
			rest: "POST /bulk",
			params: {
				// root key
				$$root: true,
				type: "object",
			},
			async handler(ctx) {
				try {
					const createdBy = ctx.meta.creator.trim().toLowerCase();

					const keys = Object.keys(ctx.params);

					for (let key of keys) {
						let value = ctx.params[key];
						key = key.trim().toUpperCase();

						if (typeof value == "object") value = JSON.stringify(value);

						// check if key exists
						const [config] = await this.adapter.db.query(
							`SELECT * FROM configs WHERE key = '${key}' AND createdBy = '${createdBy}'`
						);

						if (config.length > 0) {
							// update
							await this.adapter.db.query(
								`UPDATE configs SET value = '${value}' WHERE key = '${key}' AND createdBy = '${createdBy}'`
							);
						} else {
							// create
							await this.adapter.insert({
								key,
								value,
								createdBy,
							});
						}
					}

					return {
						code: 200,
						i18n: "CONFIGS_SET",
						data: {
							keys: keys.map((key) => key.trim().toUpperCase()),
						},
					};
				} catch (error) {
					console.error(error);
					return {
						code: 500,
					};
				}
			},
		},
		unset: {
			permission: ["api.v1.config.unset"],
			rest: "DELETE /unset",
			params: {
				key: {
					type: "string",
					min: 3,
					optional: true,
				},
			},
			async handler(ctx) {
				try {
					const createdBy = ctx.meta.creator.trim().toLowerCase();
					let { key } = ctx.params;

					if (key) {
						key = key.trim().toUpperCase();

						await this.adapter.db.query(
							`DELETE FROM configs WHERE key = '${key}' AND createdBy = '${createdBy}'`
						);
					} else {
						await this.adapter.db.query(
							`DELETE FROM configs WHERE createdBy = '${createdBy}'`
						);
					}

					return {
						code: 200,
						i18n: "CONFIG_UNSET",
					};
				} catch (error) {
					return {
						code: 500,
					};
				}
			},
		},
	},

	/**
	 * Events
	 */
	events: {
		// set event
		"config.set": {
			async handler(ctx: any) {
				let { key, value, createdBy } = ctx.params;

				if (!createdBy || typeof createdBy != "string" || createdBy.length == 0)
					return;

				if (!key || typeof key != "string" || key.length == 0) return;

				if (
					!value ||
					typeof value != "object" ||
					Object.keys(value).length == 0
				)
					return;

				key = key.trim().toUpperCase();
				createdBy = createdBy.trim().toLowerCase();

				// if typeof value is object, stringify it
				if (typeof value == "object") value = JSON.stringify(value);

				// 1. check key exists by createdBy
				// 2. if exists, update
				// 3. if not exists, create

				try {
					const [config] = await this.adapter.db.query(
						`SELECT * FROM configs WHERE key = '${key}' AND createdBy = '${createdBy}'`
					);

					if (config.length > 0) {
						// update
						await this.adapter.db.query(
							`UPDATE configs SET value = '${value}' WHERE key = '${key}' AND createdBy = '${createdBy}'`
						);
					} else {
						// create
						await this.adapter.insert({
							key,
							value,
							createdBy,
						});
					}
				} catch (error) {
					//
				}
			},
		},
		"config.unset": {
			async handler(ctx: any) {
				let { key, createdBy } = ctx.params;

				if (!createdBy || typeof createdBy != "string" || createdBy.length == 0)
					return;

				createdBy = createdBy.trim().toLowerCase();

				// method 1:
				// 1. check key exists by createdBy
				// 2. if exists, delete
				// method 2:
				// 1. if just createdBy is passed and key is not passed, delete all keys by createdBy
				try {
					if (key) {
						if (typeof key != "string" || key.length == 0) return;

						key = key.trim().toUpperCase();

						await this.adapter.db.query(
							`DELETE FROM configs WHERE key = '${key}' AND createdBy = '${createdBy}'`
						);
					} else {
						await this.adapter.db.query(
							`DELETE FROM configs WHERE createdBy = '${createdBy}'`
						);
					}
				} catch (error) {}
			},
		},
	},

	/**
	 * Methods
	 */
	methods: {},

	/**
	 * Service created lifecycle event handler
	 */
	// created() {},

	/**
	 * Service started lifecycle event handler
	 */
	// started() { },

	/**
	 * Service stopped lifecycle event handler
	 */
	// stopped() { }
};

export = Service;
