Feature: TestAT basic example

    Scenario Outline: Test example process exits successfully
      Given a config file <config_file>
      When execute example app file in PySpark
      Then result should be <exit_code>

      Scenarios:
        | config_file      | exit_code |
        | application.conf | 0         |

    Scenario Outline: Test output dataFrame columns
      Given an output dataFrame located at path <output_path>
      When read the output dataFrame
      Then total columns should be equal to <tot_columns>

      Scenarios:
        | output_path                       | tot_columns |
        | resources/data/output/final_table | 13          |

    Scenario Outline: Test output dataFrame columns
      Given an output dataFrame located at path <output_path>
      When read the output dataFrame
      Then <column_name> column should <comparator> <value>

      Scenarios:
        | output_path                       | column_name | comparator  | value |
        | resources/data/output/final_table | edad        | be gr or eq | 30    |
        | resources/data/output/final_table | edad        | be lr or eq | 50    |
        | resources/data/output/final_table | vip         | be eq       | true  |
        | resources/data/output/final_table | fec_alta    | be like     | [0-9]{4}-[0-9]{2}-[0-9]{2} |

    Scenario Outline: Test output dataFrame columns
      Given an output dataFrame phones_customers located at path <output_path>
      When read the output dataFrame phones_customers
      Then total columns phones_customers should be equal to <tot_columns>

      Scenarios:
        | output_path                       | tot_columns |
        | resources/data/output/date_table  | 19          |

    Scenario Outline: Test output dataFrame columns
      Given an output dataFrame phones_customers located at path <output_path>
      When read the output dataFrame phones_customers
      Then <column_name> column phones_customers should <comparator> <value>

      Scenarios:
        | output_path                       | column_name  | comparator  | value |
        | resources/data/output/date_table | country_code | be eq        | CH    |
        | resources/data/output/date_table | country_code | be eq        | IT    |
        | resources/data/output/date_table | country_code | be eq        | CZ    |
        | resources/data/output/date_table | country_code | be eq        | DK    |
        | resources/data/output/date_table | jwk_date     | be like      | [0-9]{4}-[0-9]{2}-[0-9]{2} |
        | resources/data/output/date_table | country_code | be eq 2      | MX    |
        | resources/data/output/date_table | country_code | be eq 2      | CA    |
        | resources/data/output/date_table | country_code | be eq 2      | BR    |
        | resources/data/output/date_table | country_code | be eq 2      | US    |
        | resources/data/output/date_table | country_code | be eq 2      | PE    |
        | resources/data/output/date_table | country_code | be eq 2      | JP    |
        | resources/data/output/date_table | country_code | be eq 2      | NZ    |
        | resources/data/output/date_table | country_code | be eq 2      | CO    |
        | resources/data/output/date_table | country_code | be eq 2      | AR    |
