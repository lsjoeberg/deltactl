use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::{
    char,
    complete::{alpha1, alphanumeric1, one_of, space0},
};
use nom::combinator::recognize;
use nom::multi::{many0_count, many1_count};
use nom::number::complete::recognize_float;
use nom::sequence::{delimited, pair, preceded, terminated};
use nom::{IResult, Parser};

fn filter_operator(input: &str) -> IResult<&str, &str> {
    terminated(
        preceded(
            space0,
            alt((
                tag("="),
                tag("!="),
                tag(">="),
                tag(">"),
                tag("<="),
                tag("<"),
                tag_no_case("in"),
                tag_no_case("not in"),
            )),
        ),
        space0,
    )
    .parse(input)
}

fn column_name(input: &str) -> IResult<&str, &str> {
    recognize(pair(
        alt((alpha1, tag("_"))),
        many0_count(alt((alphanumeric1, tag("_")))),
    ))
    .parse(input)
}

fn filter_field(input: &str) -> IResult<&str, &str> {
    alt(
        (
            recognize_float,                              // e.g. 42.42
            recognize(many1_count(one_of("0123456789"))), // e.g. 123
            delimited(
                char('\''), // must discard valid; i.e. "'2021-01-01'" -> "2021-01-01"
                recognize(many1_count(alt((
                    alphanumeric1,
                    tag("_"),
                    tag("-"),
                    tag("."),
                )))),
                char('\''),
            ),
        ), // e.g. 'some_string'
           // todo: IN (), NOT IN ()
    )
    .parse(input)
}

pub fn filter_condition(input: &str) -> Result<(&str, &str, &str), anyhow::Error> {
    let res = (
        preceded(space0, column_name),
        preceded(space0, filter_operator),
        preceded(space0, filter_field),
    )
        .parse(input);

    match res {
        Ok((_, value)) => Ok(value),
        Err(_) => Err(anyhow::anyhow!("invalid partition filter: {input}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_column_name() {
        let (_, got) = column_name("my_column").unwrap();
        assert_eq!(got, "my_column");
    }

    #[test]
    fn test_operator_isolated() {
        let operators = ["=", "!=", ">=", ">", "<=", "<", "in", "not in"];
        for op in operators {
            let (_, got) = filter_operator(op).unwrap();
            assert_eq!(got, op);
        }
    }

    #[test]
    fn test_operator_spaced() {
        let ops_isolated = ["=", "!=", ">=", ">", "<=", "<", "in", "not in"];
        let ops_input = [" =", "!=  ", " >= ", " >", "<=  ", "<", " in ", " not in "];
        for (i, op) in ops_input.into_iter().enumerate() {
            let (_, got) = filter_operator(op).unwrap();
            assert_eq!(got, ops_isolated[i]);
        }
    }

    #[test]
    fn test_filter_field_isolated() {
        let fields = ["1.42", "123", "'some_string'", "'2024-02-21'"];
        for field in fields {
            let (_, got) = filter_field(field).unwrap();
            assert_eq!(got, field);
        }
    }

    #[test]
    fn test_filter_condition() {
        let inputs = ["id > 200"];
        let expected = [("id", ">", "200")];
        for (i, input) in inputs.into_iter().enumerate() {
            let got = filter_condition(input).unwrap();
            assert_eq!(got, expected[i]);
        }
    }
}
