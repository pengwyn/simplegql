use std::{fs, fmt, collections::{HashMap, HashSet, BTreeMap}};

use crate::utils::*;
use crate::handlers::{GQLValueType, GQLValueTypeAnnotated};

// use graphql_parser::schema;
use graphql_parser::schema::{
    Definition,
    TypeDefinition,
    Value as schema_Value,
};
#[allow(non_camel_case_types)]
pub type schema_Value2<'a> = schema_Value<'a, String>;

type ObjectType<'a> = graphql_parser::schema::ObjectType<'a, String>;
type ScalarType<'a> = graphql_parser::schema::ScalarType<'a, String>;
type ParserField<'a> = graphql_parser::schema::Field<'a, String>;


#[derive(Debug)]
pub struct APIDefinition {
    pub types: HashMap<String,Type>,
    pub enums: HashMap<String,Enum>,
    pub scalars: HashMap<String,Scalar>,
}

#[derive(Debug)]
pub struct Enum {
    pub name: String,
    pub values: Vec<String>,
}

#[derive(Debug)]
pub struct Type {
    pub name: String,
    pub fields: Vec<Field>,
}

#[derive(Debug)]
pub struct FuncName(String); 

#[derive(Debug)]
pub struct Scalar {
    pub name: String,
    pub backed_by: GQLValueType,
    pub to_db: Option<FuncName>,
    pub to_gql: Option<FuncName>,
}

pub type GQLIndexMap = BTreeMap<String, schema_Value2<'static>>;
#[derive(Clone)]
pub struct DynamicResolver {
    pub name: String,
    pub args: GQLIndexMap,
}

#[derive(Clone)]
pub struct Field {
    pub name: String,
    pub internal_name: String,
    pub typ: GQLValueTypeAnnotated,
    pub incoming_source_name: Option<String>,

    // These will have defaults given by their accessors
    pub unique: Option<bool>,
    pub searchable: Option<bool>,

    pub dynamic_resolver: Option<DynamicResolver>,
    pub default: Option<String>,
    pub hints: Option<HashMap<String, schema_Value2<'static>>>,
}

impl Field {
    fn new(name: String, typ: GQLValueTypeAnnotated) -> Field {
        Field { name: name.clone(),
                internal_name: name.clone(),
                typ,
                incoming_source_name: None,
                unique: None,
                searchable: None,
                dynamic_resolver: None,
                hints: None,
                default: None,
        }
    }

    pub fn required(&self) -> bool {
        self.typ.is_nn
    }
    pub fn is_list(&self) -> bool {
        self.typ.is_list
    }
    pub fn is_dynamic(&self) -> bool {
        self.dynamic_resolver.is_some()
    }
    pub fn is_object(&self) -> bool {
        matches!(self.typ.kind, GQLValueType::Object(_))
    }
    pub fn is_incoming(&self) -> bool {
        self.incoming_source_name.is_some()
    }
    pub fn unique(&self) -> bool {
        self.unique.unwrap_or(false)
    }
    pub fn searchable(&self) -> bool {
        self.searchable.unwrap_or(false)
    }
}

impl fmt::Debug for Field {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Field(\"{}\"", self.name)?;

        // I should try a macro with this - the helper macros would be good
        let opt_list: [(_, fn(_) -> _); 4] = [
            ("required", Field::required),
            ("list", Field::is_list),
            ("unique", Field::unique),
            ("searchable", Field::searchable)
        ];
        for (name, prop) in opt_list {
            if prop(self) {
                write!(f, ", {name}")?;
            }
        }
        write!(f, "typ: {:?}", self.typ)?;
        write!(f, ")")
    }
}
            


fn handle_type(obj: ObjectType) -> ResultAll<Type> {
    let fields: ResultAll<Vec<Field>> = obj.fields.iter()
        .map(parse_field)
        .collect();
    let fields = fields?;

    Ok(Type { name: obj.name.clone(),
           fields })
}

fn parse_field(parser_f: &ParserField) -> ResultAll<Field> {
    let name = parser_f.name.clone();

    let typ = from_parsed_type(parser_f.field_type.clone());

    let mut field = Field::new(name, typ);

    fn args_as_checked_dict(
        args: Vec<(String,schema_Value2)>,
        opts: &[&str],
    ) -> HashMap<String, schema_Value2<'static>> {
        let mut d = HashMap::<String, schema_Value2>::new();
        for (s,v) in args {
            d.insert(s.clone(), v.into_static());
        }
        let keys: HashSet<String> = d.keys().cloned().collect();
        let required: HashSet<String> = opts.iter().map(|x| x.to_string()).collect();
        if keys != required {
            panic!("Arguments don't match: {:?}", opts);
        }
        d
    }

    fn single_arg_directive<'a>(args: &Vec<(String, schema_Value2<'a>)>, name: &str) -> ResultAll<schema_Value2<'a>> {
        if args.len() == 0 {
            Err(format!("Directive requires a single argument '{name}')"))?;
        } else if args.len() > 2 {
            let all_args = args.iter()
                .map(|x| format!("'{}'", x.0))
                .collect::<Vec<String>>()
                .join(", ");
            Err(format!("Directive can't take multiple arguments, found [{all_args}])"))?;
        }

        let arg = &args[0];
        if arg.0 != name {
            Err(format!("Expected argument '{name}' but found '{}'", arg.0))?;
        }

        Ok(arg.1.clone())
    }

    fn single_string_directive(args: &Vec<(String, schema_Value2)>, name: &str) -> ResultAll<String> {
        let arg = single_arg_directive(&args, name)?;
        match arg {
            schema_Value2::String(s) => Ok(s),
            other => {
                dbg!(other);
                Err(format!("Directive argument '{name}' is not a String"))?},
        }
    }

    for dir in parser_f.directives.iter() {
        match dir.name.as_str() {
            "search" => field.searchable = Some(true),
            "unique" => field.unique = Some(true),
            "dynamic" => {
                // let s = single_string_directive(&dir.arguments, "hook")?;
                let d = args_as_checked_dict(dir.arguments.clone(), &["hook", "args"]);
                let name = match &d["hook"] {
                    schema_Value2::String(x) => x.to_string(),
                    _ => panic!("hook is not a string"),
                };
                let args = match &d["args"] {
                    schema_Value2::Object(x) => x,
                    _ => panic!("hook is not a string"),
                };
                field.dynamic_resolver = Some(DynamicResolver{name, args: args.clone()});
            },
            "relation" => {
                let s = single_string_directive(&dir.arguments, "name")?;
                field.internal_name = s;
            },
            "default" => {
                let s = single_string_directive(&dir.arguments, "hook")?;
                field.default = Some(s);
            },
            "incoming" => {
                let s = single_string_directive(&dir.arguments, "src_field")?;
                field.incoming_source_name = Some(s);
            },
            "db" => field.hints = Some(dir.arguments
                                       .iter()
                                       .map(|x| (x.0.to_owned(), x.1.into_static()))
                                       .collect()),
            _ => return Err(format!("Don't understand field directive {}", dir.name).into())
        };
    }

    Ok(field)
}

fn handle_scalar(obj: ScalarType) -> ResultAll<Scalar> {
    let mut backed_by: Option<_> = None;
    let mut to_db: Option<_> = None;
    let mut to_gql: Option<_> = None;
    for dir in &obj.directives {
        match dir.name.as_str() {
            "DBDef" => {
                for arg in &dir.arguments {
                    let val = match &arg.1 {
                        schema_Value::String(x) => x,
                        _ => panic!("Invalid argument for {}", arg.0),
                    };
                    match arg.0.as_str() {
                        "backedBy" => {
                            backed_by = Some(kind_from_str(&val, true));
                        },
                        "toDB" => {
                            to_db = Some(val.to_owned());
                        },
                        "toGQL" => {
                            to_gql = Some(val.to_owned());
                        },
                        arg => panic!("Invalid argument to @DBDef directive: '{}'", arg),
                    }
                }
            },
            _ => panic!("Don't understand directive {}", dir.name),
        }
    }

    let scalar = Scalar {
        name: obj.name.clone(),
        backed_by: backed_by.expect("No backedBy given"),
        to_db: to_db.map(|x| FuncName(x)),
        to_gql: to_gql.map(|x| FuncName(x)),
    };

    Ok(scalar)
}

pub fn parse_schema(filename: &str) -> ResultAll<APIDefinition> {
    let contents = fs::read_to_string(filename).expect("Couldn't open schema file");

    parse_schema_string(&contents)
}

pub fn parse_schema_string(contents: &str) -> ResultAll<APIDefinition> {
    let ast = graphql_parser::schema::parse_schema::<String>(&contents)?;

    let mut types: Vec<Type> = Vec::new();
    let mut enums: Vec<Enum> = Vec::new();
    let mut scalars: Vec<Scalar> = Vec::new();

    for def in ast.definitions {
        match def {
            Definition::DirectiveDefinition(_) => {
                return Err("Can't allow directive definitions".into());
            },
            Definition::TypeDefinition(TypeDefinition::Object(typ)) => {
                types.push(handle_type(typ)?);
            },
            Definition::TypeDefinition(TypeDefinition::Enum(x)) => {
                let values = x.values.iter()
                    .map(|x| x.name.clone())
                    .collect();

                enums.push(Enum{ name: x.name,
                                 values });
            },
            Definition::TypeDefinition(TypeDefinition::Scalar(scalar)) => {
                scalars.push(handle_scalar(scalar)?);
            },
            Definition::TypeDefinition(TypeDefinition::Union(_) | TypeDefinition::Interface(_) | TypeDefinition::InputObject(_)) => { return Err("Not allowed".into()); }
            _ => {
                println!("Unhandled kind");
            }
        }
    }

    // Have to create a separate data structure just to query the same data structure.

    let name_map: HashMap<_,_> = types.iter()
        .map(|x| (x.name.clone(), GQLValueType::Object(x.name.clone())))
        .chain(enums.iter()
               .map(|x| (x.name.clone(), GQLValueType::Enum(x.name.clone()))))
        .chain(scalars.iter()
               .map(|x| (x.name.clone(), GQLValueType::CustomScalar(x.name.clone()))))
        .collect();

    let find_obj = |name: &String| -> GQLValueType {
        return name_map.get(name)
            .unwrap_or_else(|| panic!("Could not find object with name '{name}' in types or enums"))
            .clone();
    };

    // Now reassign all "object" fields so they are explicitly object/enum fields
    for typ in &mut types {
        for field in &mut typ.fields {
            if let GQLValueType::NamedType(obj_name) = &field.typ.kind {
                field.typ.kind = find_obj(&obj_name);
            }
        }
    }
    
    let types: HashMap<_,_> = types.into_iter()
        .map(|x| (x.name.clone(), x))
        .collect();
    let enums: HashMap<_,_> = enums.into_iter()
        .map(|x| (x.name.clone(), x))
        .collect();
    let scalars: HashMap<_,_> = scalars.into_iter()
        .map(|x| (x.name.clone(), x))
        .collect();
    let api = APIDefinition{types, enums, scalars};

    Ok(api)
}


#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn parse_test() -> ResultAll<()> {
        let api = parse_schema_string(r###"
    type Taxon {
        name: String!
        rank: Rank!
        observations: [Observation!]!
        description: String
    }

    enum Rank {
        Order,
        Family,
        Species,
    }

    type Observation {
        filename: String!
        parent: Taxon @incoming(src_field: "observations")
    }"###
        )?;

        Ok(())
    }

}

fn from_parsed_type(parser_type: graphql_parser::schema::Type<String>) -> GQLValueTypeAnnotated {

    let mut rec_parser_type = &parser_type;

    let mut typ_name = String::from("");
    let mut is_list = false;
    let mut is_nn = false;
    let mut is_inner_nn = false;
    loop {
        use graphql_parser::query::Type;
        match rec_parser_type {
            Type::NamedType(n) => {
                n.clone_into(&mut typ_name);
                break
            },
            Type::NonNullType(x) => {
                rec_parser_type = x.as_ref();
                if is_list {
                    is_inner_nn = true;
                } else {
                    is_nn = true;
                }
            },
            Type::ListType(x) => {
                rec_parser_type = x.as_ref();
                is_list = true;
            }
        }
    }

    let kind = kind_from_str(&typ_name, false);

    let typ = GQLValueTypeAnnotated{kind, is_list, is_nn, is_inner_nn};

    typ
}

fn kind_from_str(s: &str, only_scalars: bool) -> GQLValueType {
    match s {
        gqld::TypeRef::INT => GQLValueType::Integer,
        gqld::TypeRef::FLOAT => GQLValueType::Float,
        gqld::TypeRef::STRING => GQLValueType::String,
        gqld::TypeRef::BOOLEAN => GQLValueType::Boolean,
        gqld::TypeRef::ID => GQLValueType::Integer,
        name => {
            if only_scalars {
                panic!("Can't parse scalar kind {}", name);
            } else {
                GQLValueType::NamedType(name.to_string())
            }
        }
    }
}