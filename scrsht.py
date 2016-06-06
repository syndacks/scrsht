
import argparse
import httplib2
import json
import os

from apiclient import discovery
import oauth2client
from oauth2client import client
from oauth2client import tools

SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'
CLIENT_SECRET_FILE = 'client_secret.json'

parser = argparse.ArgumentParser(parents=[tools.argparser])
parser.add_argument('spreadsheet_id')
args = parser.parse_args()

def get_credentials():
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'endlessm_spreadsheet_scraper.json')

    store = oauth2client.file.Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        credentials = tools.run_flow(flow, store, args)
        print('Storing credentials to %s' % (credential_path,))
    return credentials

def fetch_spreadsheet(spreadsheet_id):
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('sheets', 'v4', http=http, discoveryServiceUrl='https://sheets.googleapis.com/$discovery/rest?version=v4')
    return service.spreadsheets().get(spreadsheetId=spreadsheet_id, includeGridData=True).execute()

def fetch_json():
    with open('out.json', 'r') as f:
        return json.load(f)

def get_sheets(data):
    sheets = {}
    for sheet in data['sheets']:
        name = sheet['properties']['title']
        sheets[name] = sheet['data']
    return sheets

def get_row_data_contents(sheet_data):
    def cell_data(cell):
        if 'effectiveValue' not in cell:
            return None

        effective_value = cell['effectiveValue']
        if 'stringValue' in effective_value:
            return effective_value['stringValue'].strip()

        assert len(effective_value) == 1
        return effective_value.values()[0]

    rows = sheet_data[0]['rowData']
    rows = [[cell_data(cell) for cell in row['values']] for row in rows]
    return rows

def should_be_array(key):
    return key in ['childTags', 'tags']

def mr_data_convert(sheet_data):
    row_data = get_row_data_contents(sheet_data)
    header, contents = row_data[0], row_data[1:]

    out_rows = []
    for row_contents in contents:
        out_row = {}

        for i, key in enumerate(header):
            if key is None:
                continue

            if i >= len(row_contents):
                continue

            if row_contents[i] is None:
                continue

            out_row.setdefault(key, []).append(row_contents[i])

        for key, val in out_row.iteritems():
            if len(val) == 1 and not should_be_array(key):
                out_row[key] = val[0]

        if out_row != {}:
            out_rows.append(out_row)

    return out_rows

def main():
    data = fetch_spreadsheet(args.spreadsheet_id)
    # data = fetch_json()

    sheets = get_sheets(data)

    # app_json = mr_data_convert(sheets['app.json'])[0]
    content = mr_data_convert(sheets['content'])
    sets = mr_data_convert(sheets['sets'])

    db_json = {}
    db_json['content'] = content
    db_json['sets'] = sets

    with open('db.json', 'w') as f:
        json.dump(db_json, f, sort_keys=True, indent=True, separators=(',', ': '))

if __name__ == '__main__':
    main()
