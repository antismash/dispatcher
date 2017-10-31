"""Handle email sending"""
import aiosmtplib
from email.mime.text import MIMEText


class EmailConfig:
    """Class collection all email-related configuration"""
    __slots__ = (
        'authenticate',
        'base_url',
        'enabled',
        'encrypt',
        'host',
        'password',
        'sender',
        'error',
        'tool',
        'user',
    )

    def __init__(self, **kwargs):
        for entry in EmailConfig.__slots__:
            if entry in ('enabled', 'authenticate'):
                self.__setattr__(entry, kwargs.get(entry, False))
                continue
            self.__setattr__(entry, kwargs.get(entry, None))

    @classmethod
    def from_env(cls, env):
        """Create an EmailConfig from an Env object"""
        kwargs = dict(tool=env('ASD_TOOL_NAME'), base_url=env('ASD_BASE_URL'))

        if env('ASD_EMAIL_HOST') and env('ASD_EMAIL_FROM') and env('ASD_EMAIL_ERROR'):
            kwargs['enabled'] = True
            kwargs['host'] = env('ASD_EMAIL_HOST')
            kwargs['sender'] = env('ASD_EMAIL_FROM')
            kwargs['error'] = env('ASD_EMAIL_ERROR')

        if env('ASD_EMAIL_USER') and env('ASD_EMAIL_PASSWORD'):
            kwargs['authenticate'] = True
            kwargs['user'] = env('ASD_EMAIL_USER')
            kwargs['password'] = env('ASD_EMAIL_PASSWORD')

        if env('ASD_EMAIL_ENCRYPT'):
            option = env('ASD_EMAIL_ENCRYPT').lower()
            if option not in ('tls', 'starttls', 'no'):
                raise ValueError("Invalid ASD_EMAIL_ENCRYPT option: {}. Valid: 'tls', 'starttls', or 'no'".format(option))
            kwargs['encrypt'] = option

        return cls(**kwargs)


async def init_mail(app):
    """"Set up the smtp class"""
    conf = app['mail_conf']
    if not conf.enabled:
        app.logger.debug("mails not configured")
        app['smtp'] = None
        return

    port = 587
    use_tls = False

    if conf.encrypt == 'tls':
        port = 465
        use_tls = True

    smtp = aiosmtplib.SMTP(hostname=conf.host, port=port, use_tls=use_tls, loop=app.loop)
    app['smtp'] = smtp


async def close_mail(app):
    """Close the connection"""
    smtp = app['smtp']
    if not smtp:
        return

    if not smtp.is_connected:
        return

    await smtp.quit()


async def send_job_mail(app, job, warnings, errors):
    """Send a 'your job finished' mail"""
    if not job.email:
        app.logger.debug('no email configured')
        return

    mail_conf = app['mail_conf']
    if not mail_conf.enabled:
        app.logger.debug("Not sending emails, no mail server configured")
        return

    if job.state == 'done':
        action_string = success_template.format(j=job, c=mail_conf)
    else:
        action_string = failure_template.format(c=mail_conf, errors="\n".join(errors))

    message_text = message_template.format(j=job, c=mail_conf, action_string=action_string)

    message = MIMEText(message_text)
    message['From'] = mail_conf.sender
    message['To'] = job.email
    message['Subject'] = "Your {c.tool} job {j.job_id} finished.".format(j=job, c=mail_conf)

    await _send_mail(app, message)


async def send_error_mail(app, job, warnings, errors, backtrace):
    """Send an error report to make debugging easier"""
    mail_conf = app['mail_conf']
    if not mail_conf.enabled:
        app.logger.debug("Not sending error emails, no mail server configured")
        return

    message_text = error_message_template.format(j=job, c=mail_conf, errors='\n'.join(errors),
                                                 warnings='\n'.join(warnings),
                                                 backtrace='\n'.join(backtrace))
    message = MIMEText(message_text)
    message['From'] = mail_conf.sender
    message['To'] = mail_conf.error
    message['Subject'] = "[{j.jobtype}] {c.tool} job {j.job_id} failed.".format(j=job, c=mail_conf)

    await _send_mail(app, message)


async def _send_mail(app, message):
    """Send a MIMEText message using an existing SMTP object"""
    conf = app['mail_conf']
    smtp = app['smtp']

    await smtp.connect()
    if conf.encrypt == 'starttls':
        app.loggger.debug('running STARTTLS handshake')
        await smtp.starttls()

    if conf.authenticate:
        await smtp.login(username=conf.user, password=conf.password)

    await smtp.send_message(message)

    await smtp.quit()


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

Please contact {c.sender} to resolve the issue."""

error_message_template = """The {c.tool} job {j.job_id} has failed.
Dispatcher: {j.dispatcher}
Input file: {c.base_url}/upload/{j.job_id}/{j.filename}
GFF file: {c.base_url}/upload/{j.job_id}/{j.gff3}
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
