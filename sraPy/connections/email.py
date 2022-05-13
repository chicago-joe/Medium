#!/usr/bin/env python
# coding=utf-8

import os, pendulum
import pandas as pd
import prefect
from prefect.tasks.notifications import EmailTask
from prefect.tasks.aws import AWSSecretsManager

# --------------------------------------------------------------------------------------------------
# mysql class

class SRAMailer(EmailTask):
    """
        Python Class for connecting  with SRA email subscriptions

        --- Example usage of task and flow: ---

        import yfinance as yf
        from prefect import task, Flow
        from sraPy.connections.email import SRAMailer

        @task()
        def download_data(ticker="SPY"):
            df = yf.download(tickers = ticker)
            return df.style.render()

        with Flow("test email flow") as flow:
            # mailer = SRAMailer()
            data = download_data("SPY")
            SRAMailer()(email_to='jloss@spiderrockadvisors.com',email_to_cc = 'parsa.shoa@spidderockadvisors.com',msg_html = data)

        if __name__ == '__main__':
            flow.run()
    """

    __instance = None
    __mail_from = None
    __config = None
    __name = None
    email_to = None
    email_to_cc = None
    email_to_bcc = None
    subject = None
    msg_plain = None
    msg_html = None
    attachments = None

    def __init__(self, mail_from = 'SRAOPSDESK', **kwargs):
        self.__get_config()
        super().__init__(**kwargs)

    ## End def __init__

    def __get_config(self):
        config = AWSSecretsManager(boto_kwargs = { "region_name":"us-east-2" }).run(secret = "SRAOPSDESK_EMAIL_CONFIG")
        self.__config = config

    ## End def __get_config

    def run(self, email_to = None, email_to_cc = None, email_to_bcc = None, subject = None, msg_html = None, msg_plain = None, attachments = None):

        self.email_to = email_to
        self.email_to_bcc = email_to_bcc
        self.email_to_cc = email_to_cc
        self.msg_html = msg_html
        self.msg_plain = msg_plain
        self.attachments = attachments

        if not subject:
            self.subject = os.path.basename('email.py') + " " + pendulum.now("America/Chicago").to_datetime_string()
        else:
            self.subject = subject

        if self.email_to is not None:
            if type(self.email_to) != list:
                self.email_to = [self.email_to]
            self.email_to = ";".join(self.email_to)
        if self.email_to_cc is not None:
            if type(self.email_to_cc) != list:
                self.email_to_cc = [self.email_to_cc]
            self.email_to_cc = ";".join(self.email_to_cc)
        if self.email_to_bcc is not None:
            if type(self.email_to_bcc) != list:
                self.email_to_bcc = [self.email_to_bcc]
            self.email_to_bcc = ";".join(self.email_to_bcc)

        email_notification = EmailTask(
                smtp_server = self.__config.get('smtp_server'),
                smtp_port = self.__config.get('smtp_port'),
                smtp_type = self.__config.get('smtp_type'),
                email_from = self.__config.get("email_from"),
                email_to = self.email_to,
                email_to_cc = self.email_to_cc,
                email_to_bcc = self.email_to_bcc,
                subject = self.subject,
                msg = self.msg_html,
                msg_plain = self.msg_plain,
                attachments = self.attachments
        )
        return email_notification.run()
