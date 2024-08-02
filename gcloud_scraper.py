import json
import os
from datetime import datetime, date
from pprint import pprint as pretty_print
from typing import NamedTuple
from lxml import etree, html
from lxml.etree import ParserError
import requests
import pandas as pd
import pyap
import click
import phonenumbers
import re
from google.cloud import storage
from google.api_core.exceptions import NotFound
from google.oauth2 import service_account
import googleapiclient.discovery
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

white_space_pattern = re.compile(r'\s+')

class ScraperResultRow(NamedTuple):
    index: int
    accessed_timestamp: str
    entryid: int
    connection: bool
    connection_notes: str
    organization: str
    department_program: str
    url: str
    full_address_xpath: str
    preexisting_full_address: str
    raw_scraped_full_address: str
    formatted_scraped_full_address: str
    phonenum_xpath: str
    preexisting_phonenum: str
    raw_scraped_phonenum: str
    formatted_scraped_phonenum: str
    keyword_xpath: str
    preexisting_keyword: str
    raw_scraped_keyword: str

def access_google_sheet(credentials, spreadsheet_id, range_name):
    # Authenticate and access the Google Sheets API
    scoped_credentials = credentials.with_scopes([
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ])

    # Build the service
    service = googleapiclient.discovery.build('sheets', 'v4', credentials=scoped_credentials)

    # Make an API call to read data from the Google Sheet
    result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    sheet_data = result.get('values', [])
    return sheet_data

def download_credentials(bucket_name, credentials_file_name):
    # Initialize the Google Cloud Storage client
    storage_client = storage.Client()

    # Get the credentials file from the bucket
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(credentials_file_name)
    credentials_json = blob.download_as_text()

    # Load the credentials
    credentials = service_account.Credentials.from_service_account_info(json.loads(credentials_json))
    return credentials

def connect_to_website(url: str):
    headers = {
        "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/52.0.2743.82 Safari/537.36")
    }
    valid_connection = False
    try:
        response = requests.get(url, headers=headers, timeout=30, verify=False)
        etree.fromstring(response.content)
    except requests.exceptions.MissingSchema:
        response = 'The URL scheme (e.g. http or https) is missing.'
    except requests.exceptions.SSLError:
        response = 'Bad SSL Certificate'
    except requests.exceptions.ReadTimeout:
        response = 'Their server did not send any data in the allotted amount of time.'
    except requests.exceptions.Timeout:
        response = 'The request timed out.'
    except TypeError:
        response = 'Possibly a Google API error'
    except ParserError:
        response = 'Document is empty'
    except requests.exceptions.ConnectionError:
        response = "A Connection error occurred."
    except Exception as e:
        response = str(e)
    else:
        valid_connection = True

    return valid_connection, response


def parse_address_string(raw_address_str: str):
    if not raw_address_str:
        print(f'\t\tEMPTY ADDRESS STR passed="{raw_address_str}"')
        return {}

    print(f'\t\tRAW ADDRESS STR="{raw_address_str}"')

    if isinstance(raw_address_str, list):
        raw_address_str = " ".join([str(_) for _ in raw_address_str])

    raw_address_str = white_space_pattern.sub(' ', raw_address_str).strip()

    if ',' in raw_address_str:
        raw_address_str = raw_address_str.replace(',', "")

    addresses = pyap.parse(raw_address_str, country='US')

    if not addresses:
        print('\t\tCould not parse address, returning raw address string.')
        return {'full_address': raw_address_str}  # Return as dictionary

    # Assuming addresses is a list of objects that have an as_dict method:
    return addresses[0].as_dict()


def parse_phone_string(raw_phone_str: str) -> str:
    
    print(f'\t\tRAW PHONE STR="{raw_phone_str}"')

    if not raw_phone_str.strip():
        return ""

    ph_num = ""

    try:
        for m in phonenumbers.PhoneNumberMatcher(raw_phone_str, region="US"):
            ph_num = m.number
            break

        if not ph_num:
            ph_num = phonenumbers.parse(raw_phone_str, region="US")

        if ph_num:
            ph_num = phonenumbers.format_number(
                ph_num,
                phonenumbers.PhoneNumberFormat.NATIONAL
            )

    except phonenumbers.phonenumberutil.NumberParseException:
        pass

    print(f'\t\tPARSED PHONE NUM="{ph_num}"')
    return ph_num

def scrape_one_website(row: pd.Series) -> ScraperResultRow:
    print('inside scrape fx')
    idx = int(row.name) + 1
    print('*' * 50, f'\nNow processing index={idx}\nURL:\n{row[22]}\n')

    timestamp = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    
    valid_connection, response = connect_to_website(row[22])

    raw_scraped_phonenum = ''
    raw_scraped_full_address = ''
    raw_scraped_keyword = ''
    formatted_scraped_phonenum = ''
    formatted_scraped_full_address = ''

    if valid_connection:
        tree = html.fromstring(response.content)

        def collect_text_from_xpath(xpath: str) -> str:
            print(f"Attempting XPath:\n{xpath}")

            try:
                retval = tree.xpath(xpath)
            except lxml.etree.XPathEvalError:
                return ""

            def obtain_text_from_xpath_result(raw_xpath_result):
                if isinstance(raw_xpath_result, HtmlElement):
                    raw_xpath_result = raw_xpath_result.text_content()
                return str(raw_xpath_result)


            if isinstance(retval, list) and len(retval) != 0:
                result = " ".join([obtain_text_from_xpath_result(_) for _ in retval])
            else:
                result = str(retval)

            result = white_space_pattern.sub(' ', result).strip()
            return "" if result == '[]' else result

        if row[32]:#address_xpath
            raw_scraped_full_address = collect_text_from_xpath(row[33]) #address_xpath
            parsed_address = parse_address_string(raw_scraped_full_address)
            formatted_scraped_full_address = parsed_address.get('full_address', raw_scraped_full_address)

        if row[33]: #phonenum_xpath
            raw_scraped_phonenum = collect_text_from_xpath(row[34]) #phonenum_xpath
            formatted_scraped_phonenum = parse_phone_string(raw_scraped_phonenum)

        if row[36]:#keyword_xpath
            raw_scraped_keyword = collect_text_from_xpath(row[36]) #keyword_xpath

    result = ScraperResultRow(
        idx,
        timestamp,
        row[0], #entryid
        valid_connection,
        response if isinstance(response, str) else "OK",
        row[5], #organization
        row[6], #department_program
        row[22],#url
        row[33],#address_xpath
        row[16],#full_address
        raw_scraped_full_address,
        formatted_scraped_full_address,
        row[34],#phonenum_xpath
        row[19],#phonenum
        raw_scraped_phonenum,
        formatted_scraped_phonenum,
        row[36],#keyword_xpath
        row[35],#keyword
        raw_scraped_keyword
    )

    print("\n")
    pretty_print(result._asdict())

    return result

def upload_to_gcs(upload_bucket, file_name, content):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(upload_bucket)
        if not bucket.exists():
            raise NotFound(f"Bucket {upload_bucket} not found.")
        content_as_bytes = str.encode(content)
        blob = bucket.blob(file_name)
        blob.upload_from_string(content_as_bytes)
        print(f"File {file_name} uploaded to {upload_bucket}.")
    except NotFound as e:
        print(f"An error occurred: {e}")  # Handling bucket not found
    except Exception as e:
        print(f"An unexpected error occurred: {e}")  # Handling other exceptions

def send_email(email_subject, email_body, to, attachment_path):
    from_email = os.environ.get('FROM_EMAIL')
    password = os.getenv("EMAIL_PASSWORD")

    # Create a multipart message
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to
    msg['Subject'] = email_subject
    msg.attach(MIMEText(email_body, 'plain'))

    # Attach the file
    attachment = open(attachment_path, "rb")
    part = MIMEBase('application', 'octet-stream')
    part.set_payload((attachment).read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', "attachment; filename= %s" % os.path.basename(attachment_path))
    msg.attach(part)

    # Send the email
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(from_email, password)
    text = msg.as_string()
    server.sendmail(from_email, to, text)
    server.quit()


def main(spreadsheet_id, range_name, bucket_name, credentials_file_name, upload_bucket, recipient_email, email_subject, email_body):
       
    # Download credentials
    credentials = download_credentials(bucket_name, credentials_file_name)
    
    # Access Google Sheet
    sheet_data = access_google_sheet(credentials, spreadsheet_id, range_name)
    
    # Ensure the number of columns matches by trimming extra columns
    max_columns = max(len(row) for row in sheet_data)
    sheet_data = [row + [''] * (max_columns - len(row)) for row in sheet_data] 
    
    #Collect data from sheet
    df = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])  # Use header row as column names

    # Scrape data
    results = [scrape_one_website(row) for index, row in df.iterrows()]
    results_df = pd.DataFrame(results)
    results_csv = results_df.to_csv(index=False)

    # Upload results to GCS
    file_name = f"scraped_results_{date.today().strftime('%Y%m%d')}.csv"
    upload_to_gcs(upload_bucket, file_name, results_csv)

    # Send email
    attachment_path = f"/tmp/{file_name}"
    with open(attachment_path, 'w') as f:
        f.write(results_csv)

    send_email(email_subject, email_body, recipient_email, attachment_path)

def entry_point(event, context):
    # Extract settings from environment variables
    spreadsheet_id = os.getenv('SPREADSHEET_ID')
    range_name = os.getenv('RANGE_NAME')
    bucket_name = os.getenv('BUCKET_NAME')
    credentials_file_name = os.getenv('CREDENTIALS_FILE_NAME')
    upload_bucket = os.getenv('UPLOAD_BUCKET')
    recipient_email = os.getenv('RECIPIENT_EMAIL')
    email_subject = os.getenv('EMAIL_SUBJECT')
    email_body = os.getenv('EMAIL_BODY')

    # Check if all necessary variables are set
    if not all([spreadsheet_id, range_name, bucket_name, credentials_file_name, upload_bucket, recipient_email, email_subject, email_body]):
        return 'Missing one or more configuration settings in environment variables', 500

    # If you're using 'data', you might need to access data like this (example for Pub/Sub):
    #if 'data' in event:
        #message = base64.b64decode(data['data']).decode('utf-8')
        #print(f"Received message: {message}")    

    # Call main function with these settings
    main(spreadsheet_id, range_name, bucket_name, credentials_file_name, upload_bucket, recipient_email, email_subject, email_body)

    return 'Function executed successfully', 200

if __name__ == '__main__':
    import click
    @click.command()
    @click.option('--spreadsheet-id', required=True, help='The Google Sheets spreadsheet ID')
    @click.option('--range-name', required=True, help='The range of cells to read from the spreadsheet')
    @click.option('--bucket-name', required=True, help='The name of the Google Cloud Storage bucket')
    @click.option('--credentials-file-name', required=True, help='The name of the credentials file in the bucket')
    @click.option('--upload-bucket', required=True, help='The name of the bucket to upload the results to')
    @click.option('--recipient-email', required=True, help='The email address to send the results to')
    @click.option('--email_subject', required=True, help='The email subject')
    @click.option('--email_body', required=True, help='The email body')
    def cli(spreadsheet_id, range_name, bucket_name, credentials_file_name, upload_bucket, recipient_email, email_subject, email_body):
        main(spreadsheet_id, range_name, bucket_name, credentials_file_name, upload_bucket, recipient_email, email_subject, email_body)
    cli()
