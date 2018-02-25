from observatorio_laboral.model import CassandraModel


class Keyword(CassandraModel):
    """
    Class that contains the structure of a keyword.

    Attributes:
        - category          -> Category in which the keyword will be used.
        - Word              -> Word or ngram which represents the keyword
        - similars_stem     -> Similar stemmed words.
        - similars_no_stem  -> Similar words without stem preprocessing.
    """

    model_id = "keyword"
    fields = ['category', 'word', 'similars_no_stem', 'similars_stem']

    def __init__(self, keyspace, table,
                 category,
                 word,
                 similars_no_stem=[], similars_stem=[]):

        super().__init__(keyspace, table)
        self.category = category
        self.word = word
        self.similars_no_stem = similars_no_stem
        self.similars_stem = similars_stem

    # ----------------------------------------------------------------------
    # Cassandra methods
    @classmethod
    def DefineCreateTableCommand(cls, keyspace, table):
        table_creation_cmd = \
            """
            CREATE TABLE IF NOT EXISTS {0} (
            category text,
            word text,
            similars_no_stem list<text>,
            similars_stem list<text>,
            PRIMARY KEY (category, word));
            """.format(cls.table)

        cls.CreateTable(table_creation_cmd)

    @classmethod
    def DefineStatements(cls, table):
        statements = {}
        statements['insert'] = \
            """
            INSERT INTO {0}
            (category, word, similars_no_stem, similars_stem)
            VALUES
            (?, ?, ?, ?);
            """.format(table)

        statements['select'] = \
            """
            SELECT * FROM {0}
            WHERE category = ?
            """.format(table)

        statements['select_word'] = \
            """
            SELECT * FROM {0}
            WHERE category = ?
            AND word = ?;
            """.format(table)

        statements['select_all'] = \
            """
            SELECT * FROM {0};
            """.format(table)

        return statements

    @classmethod
    def FromTextNamedTuple(cls, keyspace, table, dictionary):
        return cls(keyspace=keyspace,
                   table=table,
                   category=dictionary['category'],
                   word=dictionary['word'],
                   similars_no_stem=list(dictionary['similars_no_stem']),
                   similars_stem=list(dictionary['similars_stem']),
                   )

    @classmethod
    def FromNamedTuple(cls, keyspace, table, row):
        """Match __init__ method."""
        return cls(keyspace=keyspace,
                   table=table,
                   category=row.category,
                   word=row.word,
                   similars_no_stem=row.similars_no_stem,
                   similars_stem=row.similars_stem,
                   )

    def ToTuple(self):
        """Match 'insert' statement. Check the order"""
        return (self.category,
                self.word,
                self.similars_no_stem,
                self.similars_stem,
                )
