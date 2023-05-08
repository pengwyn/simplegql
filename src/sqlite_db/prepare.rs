use tokio::sync::mpsc::error::SendTimeoutError;

use super::*;
use crate::parsing::{
    self,
    APIDefinition
};
use crate::handlers::GQLValueType;

impl SqliteDB {
    pub fn prepare_tables(&self, api: &APIDefinition, allow_migration: bool) -> ResultAll<()> {
        self.conn.execute("BEGIN TRANSACTION;").expect("Transaction begin");

        // Do enums first as types will make constraints using them.
        for (_, enm) in &api.enums {
            let sql_gen = self.make_enum_sql(enm, api, allow_migration)?;
            if !sql_gen.is_empty() {
                println!("Executing: {}", sql_gen);
                self.conn.execute(sql_gen)
                    .expect(format!("Unable to create table for {}", enm.name).as_str());
            }
        }
        for (_, typ) in &api.types {
            let mut sql_gen = self.make_table_sql(typ, api, allow_migration)?;
            
            if !sql_gen.is_empty() {
                println!("Executing: {}", sql_gen);
                self.conn.execute(sql_gen)
                    .expect(format!("Unable to create table for {}", typ.name).as_str());
            }
        }

        self.conn.execute("COMMIT;").expect("Transaction end");
        Ok(())
    }

    fn make_table_sql(&self, typ: &parsing::Type, api: &APIDefinition, allow_migration: bool) -> ResultAll<String> {
        let sql = self.make_table_sql_raw(typ, api, false)?;
        let mut ret_sqls = Vec::new();
        let maybe_row = self.conn.prepare("SELECT * FROM sqlite_master WHERE type='table' AND name=:table;")?
            .iter()
            .bind((":table", typ.name.as_str()))?
            .map(|x| x.unwrap())
            .at_most_one().unwrap_or_else(|_| panic!("More than one"));

        if let Some(row) = maybe_row {
            let prior: &str = row.read("sql");
            if prior == &sql[..sql.len()-1] {
            } else if !allow_migration {
                println!("{}\n\n{}", prior, sql);
                panic!("Prior table exists and not allowing migration to the latest version yet");
            } else {
                ret_sqls.push(self.make_table_sql_raw(typ, api, true)?);
            }
        } else {
            ret_sqls.push(sql);
        }

        // Now any edge tables to connect different types together
        for field in &typ.fields {
            if !field.typ.is_list && !matches!(field.typ.kind, GQLValueType::Object(_)) {
                continue;
            }
            if let Some(_) = field.dynamic_resolver {
                continue;
            }
            if field.is_incoming() {
                continue;
            }

            let output = self.make_edge_table(&typ.name, &field.internal_name, &field.typ, api)?;
            if let Some(x) = output {
                ret_sqls.push(x);
            }
        }

        Ok(ret_sqls.join("\n\n"))
    }

    fn make_table_sql_raw(&self, typ: &parsing::Type, api: &APIDefinition, migration: bool) -> ResultAll<String> {
        let mut field_strs = Vec::new();
        field_strs.push(format!("id {id_sql_kind} NOT NULL PRIMARY KEY {id_sql_column_def}"));

        for field in &typ.fields {
            if field.typ.is_list {
                continue;
            }
            if let Some(_) = field.dynamic_resolver {
                continue;
            }
            if field.is_incoming() {
                continue;
            }
            if let GQLValueType::Object(_) = field.typ.kind {
                continue;
            }
            
            let mut field_str = format!("\"{}\"", field.internal_name);
            let typ_str = sqlite_type(&field.typ.kind, api).unwrap();
            field_str.push_str(&format!(" {}", typ_str));
            if field.typ.is_nn {
                field_str.push_str(" NOT NULL");
            }

            field_strs.push(field_str);
        }

        let table_name = if migration {
            format!("migrate_{}", typ.name)
        } else {
            typ.name.clone()
        };

        let mut main_sql = format!("CREATE TABLE \"{table_name}\" (
{fields}
);",
                               fields=field_strs.join(",\n")
        );

        if !migration {
            return Ok(main_sql);
        }

        let real_table_name = typ.name.clone();
        
        let mut all_sqls = Vec::new();
        all_sqls.push(String::from("PRAGMA foreign_keys=OFF;"));
        all_sqls.push(main_sql);
        
        // TODO insert
        // First find all columns that existed previously
        let mut col_grab = self.conn.prepare(format!("PRAGMA table_info({});", real_table_name))?;
        let new_cols: Vec<&String> = typ.fields.iter().map(|f| &f.internal_name).collect();
        let old_cols: ResultAll<Vec<_>> = col_grab.iter()
            .map(|row| {
                let row = row?;
                let val: &str = row.read("name");
                Ok(val.to_string())
            })
            .filter_ok(|name| new_cols.iter().any(|x| *x == name))
            .collect();
        let mut old_cols = old_cols?;
        old_cols.insert(0, String::from("id"));
        if !old_cols.is_empty() {
            println!("Migrating {} across", old_cols.join(","));

            let col_str = old_cols.into_iter()
                .map(|x| format!("\"{}\"", x))
                .join(",");
            all_sqls.push(
                format!("INSERT INTO {table_name}({col_str}) SELECT {col_str} FROM {real_table_name};")
            );
        }

        all_sqls.push(format!("DROP TABLE {};", real_table_name));
        all_sqls.push(format!("ALTER TABLE {} RENAME TO {};", table_name, real_table_name));
        all_sqls.push(String::from("PRAGMA foreign_keys=ON;"));

        Ok(all_sqls.join("\n"))
    }

    fn make_edge_table(&self, src_typ_name: &str, field_name: &str, target: &GQLValueTypeAnnotated, api: &APIDefinition) -> ResultAll<Option<String>> {
        // TODO: Probably need to factor this out so handler code can read it.
        let table_name = match &target.kind {
            GQLValueType::Object(target_name) =>
                edge_table_name(src_typ_name, field_name, target_name),
            _ => field_table_name(src_typ_name, field_name),
        };

        let mut lines: Vec<String> = Vec::new();
        lines.push(format!("'source' {id_sql_kind} NOT NULL"));

        lines.push(match &target.kind {
            GQLValueType::Object(_) => format!("'target' {id_sql_kind} NOT NULL"),
            x => format!("'value' {}", sqlite_type(x, api).unwrap()),
        });
        if !target.is_list {
            lines.push("UNIQUE('source')".into());
        }

        let lines = lines.join(",\n");
        
        let sql = format!("CREATE TABLE '{table_name}' (
{lines}
);");

        let maybe_row = self.conn.prepare("SELECT * FROM sqlite_master WHERE type='table' AND name=:table;")?
            .iter()
            .bind((":table", table_name.as_str()))?
            .map(|x| x.unwrap())
            .at_most_one().unwrap_or_else(|_| panic!("More than one"));

        if let Some(row) = maybe_row {
            let prior: &str = row.read("sql");
            if prior == &sql[..sql.len()-1] {
                return Ok(None);
            } else {
                println!("{}\n\n{}", prior, sql);
                panic!("Can't update an edge table");
            }
        } else {
            return Ok(Some(sql))
        }
    }

    fn make_enum_sql(&self, enm: &parsing::Enum, api: &APIDefinition, allow_migration: bool) -> ResultAll<String> {
        let mut full = format!("CREATE TABLE ENUM_{name} (item UNIQUE);", name=enm.name);

        let maybe_row = self.conn.prepare("SELECT * FROM sqlite_master WHERE type='table' AND name=:table;")?
            .iter()
            .bind((":table", format!("ENUM_{}", enm.name).as_str()))?
            .map(|x| x.unwrap())
            .at_most_one().unwrap_or_else(|_| panic!("More than one"));

        if let Some(row) = maybe_row {
            let prior: &str = row.read("sql");
            if prior == &full[..full.len()-1] {
                // TODO: Need to check the list of enums
                return Ok(String::from(""));
            } else {
                println!("{}\n\n{}", full, prior);
                panic!("Prior table exists and we don't know how to migrate to the latest version yet");
            }
        } else {
            let full_items = enm.values.iter()
                .map(|x| format!("(\"{x}\")"))
                .join(",\n");
            full += &format!("\nINSERT INTO 'ENUM_{name}' VALUES
    {full_items};", name=enm.name);
            return Ok(full);
        }
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    
    // #[test]
    // fn generate_table() {
    //     let default_field = parsing::Field{
    //         name: String::from(""),
    //         typ: GQLValueTypeAnnotated{
    //             kind: GQLValueType::Boolean,
    //             is_nn: true,
    //             is_list: false,
    //             is_inner_nn: false,
    //         },
    //         unique: None,
    //         searchable: None,
    //         _dynamic_resolver: None,
    //         hints: vec![],
    //     };
            
                
    //     let typ = parsing::Type{
    //         name: String::from("testname"),
    //         fields: vec![
    //             parsing::Field{
    //                 name: String::from("rank"),
    //                 ..default_field.clone()
    //             },
    //             parsing::Field{
    //                 name: String::from("somelist"),
    //                 typ: GQLValueTypeAnnotated{
    //                     kind: GQLValueType::String,
    //                     is_nn: true,
    //                     is_list: true,
    //                     is_inner_nn: false,
    //                 },
    //                 ..default_field.clone()
    //             },
    //             parsing::Field{
    //                 name: String::from("optional"),
    //                 typ: GQLValueTypeAnnotated{
    //                     kind: GQLValueType::Float,
    //                     is_nn: false,
    //                     is_list: false,
    //                     is_inner_nn: false,
    //                 },
    //                 ..default_field.clone()
    //             },
    //             parsing::Field{
    //                 name: String::from("parent"),
    //                 typ: GQLValueTypeAnnotated{
    //                     kind: GQLValueType::NamedType(String::from("testname")),
    //                     is_nn: true,
    //                     is_list: false,
    //                     is_inner_nn: false,
    //                 },
    //                 ..default_field.clone()
    //             },
    //         ]
    //     };

    //     let output = make_table_sql(&typ, api);
    //     println!("{}", output);
        
    // }
}