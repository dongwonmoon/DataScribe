"""
This module centralizes all prompt templates used to generate content with an LLM.

These prompts are the "brain" of the application's AI features. Each constant
is a carefully formatted f-string that provides context, instructions, and often
few-shot examples to the language model. This helps ensure the model's output is
structured, relevant, and constrained for its specific task.
"""

# Note: The `"""..."""` block AFTER each constant is the docstring for that constant.

COLUMN_DESCRIPTION_PROMPT = """
You are a Data Analyst. Your task is to write a brief, business-focused description
for a database column (under 15 words).

Base Context:
- Table: {table_name}
- Column: {col_name}
- Type: {col_type}

Data Profile Context:
{profile_context}

Sibling Context:
Other columns in this table: {sibling_columns}

Instructions:
1.  Use the Data Profile to make your description more accurate.
2.  If 'is_unique' is True, mention it (e.g., "Unique ID...").
3.  If 'distinct_count' is low (e.g., < 10), it's likely a category (e.g., "Status of...").
4.  If 'null_ratio' is high (e.g., > 0.5), it's likely optional (e.g., "User's middle name (optional)").
5.  Just provide the description, nothing else.

Example 1 (for an 'id' column with is_unique=True):
A unique identifier for each {table_name}.

Example 2 (for a 'status' column with distinct_count=4):
The current status of the {table_name} (e.g., pending, shipped).

Description:
"""
"""
A prompt to generate a business-focused description for a database column.

Placeholders:
- `{table_name}`: The name of the table the column belongs to.
- `{col_name}`: The name of the column.
- `{col_type}`: The data type of the column.
- `{profile_context}`: A formatted string of data profiling statistics.

Design Rationale:
- **Persona**: "You are a Data Analyst" sets the context for the desired tone.
- **Constraints**: "under 15 words" and "Just provide the description" keep the output concise and clean.
- **Context Injection**: The `Data Profile Context` provides the LLM with statistical information,
  allowing it to make more intelligent and accurate descriptions.
- **Few-Shot Examples**: The examples guide the model on how to use the context (e.g., how to
  describe a unique ID or a status column).
"""

DBT_MODEL_PROMPT = """
You are a dbt expert.
Below is the SQL query that defines the '{model_name}' dbt model.
Summarize in 1-2 sentences what kind of data this model produces or aggregates from a business perspective.

```sql
{raw_sql}
```
"""
"""
A prompt to generate a high-level, business-focused summary for a dbt model.

Placeholders:
- `{model_name}`: The name of the dbt model.
- `{raw_sql}`: The raw SQL code that defines the model.

Design Rationale:
- **Persona**: "You are a dbt expert" primes the model to understand dbt-specific concepts.
- **Focus**: "from a business perspective" steers the model away from technical jargon and
  towards a functional description.
- **Brevity**: "1-2 sentences" ensures the summary is concise.
"""

DBT_MODEL_LINEAGE_PROMPT = """
You are a data architect specializing in dbt.
Below is the SQL query that defines the '{model_name}' dbt model.
Analyze the `ref()` and `source()` functions to understand its dependencies.

Generate a Mermaid.js `graph TD` (Top-Down) flowchart code that shows the lineage for this *single* model.
- Show only the direct parents (sources or refs) flowing *into* this model.
- Do not show downstream children.
- Keep it simple.

SQL:
```sql
{raw_sql}
```

---
EXAMPLE INPUT (SQL):
with orders as (
    select * from {{ ref('stg_orders') }}
),
payments as (
    select * from {{ ref('stg_payments') }}
)
select ... from orders join payments ...
---
EXAMPLE OUTPUT (Mermaid code ONLY):
```mermaid
graph TD
    A[stg_orders] --> C({model_name});
    B[stg_payments] --> C({model_name});
```
"""
"""
A prompt to generate a Mermaid.js lineage chart for a dbt model.

Placeholders:
- `{model_name}`: The name of the dbt model.
- `{raw_sql}`: The raw SQL code that defines the model.

Design Rationale:
- **Persona**: "data architect specializing in dbt" sets a specific, expert context.
- **Output Structuring**: "Generate a Mermaid.js `graph TD` flowchart code" asks for a specific,
  machine-readable output format.
- **Scoping Instructions**: The negative constraints ("Do not show downstream children") are crucial
  for preventing the model from generating an overly complex or incorrect graph.
- **Few-Shot Example**: The example provides a clear, concrete demonstration of the expected input-to-output
  transformation, which is highly effective for structured data generation.
"""

VIEW_SUMMARY_PROMPT = """
You are a professional data analyst.
Below is the SQL query that defines the database view '{view_name}'.
Summarize in 1-2 sentences what business-level information this view provides.

```sql
{view_definition}
```
"""
"""
A prompt to generate a business-level summary for a database view.

Placeholders:
- `{view_name}`: The name of the database view.
- `{view_definition}`: The SQL `CREATE VIEW` statement.

Design Rationale:
- This prompt is very similar to `DBT_MODEL_PROMPT` but is tailored for generic database views
  instead of dbt models. The core idea is the same: summarize the business purpose from the SQL code.
"""

TABLE_SUMMARY_PROMPT = """
You are a professional data analyst.
Below is a table named '{table_name}' and its columns: {column_list_str}.
Summarize in 1-2 sentences what business-level information this table likely holds.

Example (for table 'orders' with columns 'id, user_id, product_id, order_date'):
"Stores order information, linking users to the products they purchased and when."

Summary:
"""
"""
A prompt to generate a business-level summary for a database table.

Placeholders:
- `{table_name}`: The name of the database table.
- `{column_list_str}`: A comma-separated string of column names.

Design Rationale:
- Unlike views or dbt models, a base table has no SQL code to analyze. Instead, this prompt
  provides the list of column names as context, which the LLM can use to infer the table's purpose.
- The one-shot example helps guide the model's reasoning process.
"""

DBT_COLUMN_PROMPT = """
You are a senior data governance expert using dbt.
Analyze the column '{col_name}' ({col_type}) in the dbt model '{model_name}'.
This column is generated by the following SQL:

SQL context:
```sql
{raw_sql}
```

Your task is to generate a YAML snippet for a dbt `schema.yml` file.
Provide the following keys:
1.  `description`: A concise (under 20 words) business description of this column.
2.  `meta`: A meta field containing `pii: true` if the column name or context suggests it's Personally Identifiable Information (e.g., email, name, phone, ssn), otherwise `pii: false`.
3.  `tags`: A YAML list of 1-2 relevant business tags (e.g., 'user_info', 'finance', 'pii').
4.  `tests`: A YAML list of 1-2 appropriate dbt generic tests (e.g., 'not_null', 'unique'). If no specific test seems necessary, provide an empty list `[]`.

Output ONLY the YAML snippet, starting with `description:`.
Do not include any other text, explanations, or markdown fences.

---
EXAMPLE INPUT (col_name='email'):
---
EXAMPLE OUTPUT:
description: The user's unique email address for login and contact.
meta:
  pii: true
tags:
  - user_info
  - pii
tests:
  - not_null
  - unique
"""
"""
A prompt to generate a complete YAML metadata block for a dbt column.

Placeholders:
- `{col_name}`: The name of the column.
- `{col_type}`_ The data type of the column.
- `{model_name}`: The name of the dbt model.
- `{raw_sql}`: The raw SQL code of the model for context.

Design Rationale:
- **Persona**: "senior data governance expert" primes the model to think about PII, tests, and tags.
- **Output Structuring**: "generate a YAML snippet" and "Output ONLY the YAML snippet" are strong
  instructions to get machine-readable output that can be parsed directly.
- **Few-Shot Example**: The example shows the exact YAML structure required, which is critical for
  getting reliable, structured output from the LLM.
"""

DBT_DRIFT_CHECK_PROMPT = """
You are a Data Governance Auditor.
Your task is to compare the 'Existing Description' against the 'Current Data Profile'
for the column '{node_name}.{column_name}' and determine if they conflict.

-   'Existing Description': {existing_description}
-   'Current Data Profile':
{profile_context}

Does the 'Existing Description' conflict with the 'Current Data Profile'?
Answer with a single word: 'MATCH' or 'DRIFT'.

-   Respond 'DRIFT' if the description is clearly wrong (e.g., description says 'Unique ID' but 'is_unique' is False).
-   Respond 'DRIFT' if the description mentions specific categories (e.g., 'Status is A or B') but the 'distinct_count' is very high (e.g., 1000).
-   Respond 'MATCH' if the description is still plausible, even if vague.
-   Respond 'MATCH' if the profile is 'N/A' (profiling failed).

Response (MATCH or DRIFT):
"""
"""
A prompt to check for documentation drift against live data.

Placeholders:
- `{node_name}`: The name of the model or table.
- `{column_name}`: The name of the column.
- `{existing_description}`: The documentation currently in schema.yml.
- `{profile_context}`: The fresh data profile stats from the database.

Design Rationale:
- **Persona**: "Data Governance Auditor" sets the context for a critical comparison task.
- **Constrained Output**: "Answer with a single word: 'MATCH' or 'DRIFT'" makes the output
  extremely easy and reliable to parse programmatically.
- **Clear Rules**: The bullet points provide explicit rules for the model to follow when making
  its judgment, improving accuracy and consistency.
"""

LANDSCAPE_CLUSTER_HINT_PROMPT = """
You are a data analyst. Decode the meaning of a table naming-convention group so a user can start reading a large legacy database without opening every table.

Cluster: {cluster_name}
Member count: {member_count}
Member tables: {member_tables}

In one sentence, what does this group of tables most likely represent?
"""
"""
A prompt to generate a name-decoding hint for one landscape cluster.

Placeholders:
- `{cluster_name}`: The cluster key derived from the naming convention (e.g. `TBL_CUST_MST`).
- `{member_count}`: The number of tables in the cluster.
- `{member_tables}`: A comma-separated sample of member table names.

Design Rationale:
- **Brevity**: "In one sentence" keeps the hint short enough to render inline.
- **No data values**: the prompt carries table names only — no raw column
  values, consistent with the privacy boundary.
"""

LANDSCAPE_TABLE_HINT_PROMPT = """
You are a data analyst. Decode the meaning of a cryptic legacy table name so a user knows what it stores before opening it.

Table: {table_name}

In one short sentence, what does this table most likely store?
"""
"""
A prompt to generate a name-decoding hint for one core table.

Placeholders:
- `{table_name}`: The table name to decode (e.g. `TBL_CUST_MST_2021`).

Design Rationale:
- **Minimal payload**: the prompt carries only the table name — nothing else
  about the schema or its data is transmitted.
- **Brevity**: "one short sentence" matches the inline hint rendering.
"""
