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

error_message_template = """The {c.tool} job {j.job_id} has failed {status}.
Dispatcher: {j.dispatcher}-{c.version}
Input file: {c.base_url}/upload/{j.job_id}/input/{j.filename}
GFF file: {c.base_url}/upload/{j.job_id}/input/{j.gff3}
Log file: {c.base_url}/upload/{j.job_id}/{j.job_id}.log
Side-loaded file: {c.base_url}/upload/{j.job_id}/input/{j.sideload}
Original ID: {j.original_id}
User email: {j.email}
State: {j.state}
Handlers: {j.trace}
Errors:
{errors}

Warnings:
{warnings}

Backtrace:
{backtrace}
"""
