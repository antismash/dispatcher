"""Message templates for antiSMASH notifications."""

message_template = """Dear {c.tool} user,

The {c.tool} job {j.job_id} you submitted on {j.added} with the filename
'{j.filename}' has finished with status {j.state}.

{action_string}

If you found {c.tool} useful, please check out
{c.base_url}/#!/about
for information on how to cite {c.tool}.
"""

success_template = """You can find the results on
{c.base_url}/upload/{j.job_id}/index.html

Results will be kept for one month and then deleted automatically.
"""

failure_template = """It produced the following error messages:
{errors}

Please contact {c.support} to resolve the issue."""

error_message_template = """The {c.tool} job {j.job_id} has failed.
Dispatcher: {j.dispatcher}
Input file: {c.base_url}/upload/{j.job_id}/input/{j.filename}
GFF file: {c.base_url}/upload/{j.job_id}/input/{j.gff3}
Log file: {c.base_url}/upload/{j.job_id}/{j.job_id}.log
User email: {j.email}
State: {j.state}
Errors:
{errors}

Warnings:
{warnings}

Backtrace:
{backtrace}
"""

