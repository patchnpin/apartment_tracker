import requests
import shutil
from bs4 import BeautifulSoup
import pandas as pd
import traceback
from datetime import date
import re
import os

try: 
    def get_more_details(unit_num, spaces_id, base_url):
        url = f"{base_url}?spaces_tab=unit-detail&detail={spaces_id}"
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')

        amenities_section = soup.find('detail', id='spaces-unit-amenities')
        if not amenities_section:
            return {}

        floor     = int(str(unit_num)[-2])
        garage    = 0
        fireplace = False
        building  = str(unit_num)[:2]
        lake_view = (building == '07' or building == '11') and (int(unit_num) % 2 == 0)
        tour_available = bool(soup.find('i', class_='spaces__icon-virtual-tour'))
        unit_url = url

        for li in amenities_section.find_all('li'):
            text = li.get_text(strip=True).lower()

            match = re.search(r'(\d+)(?:st|nd|rd|th)\s+floor', text)
            if match:
                floor = int(match.group(1))

            if 'attached 2-car garage' in text:
                garage = 2
            elif 'attached garage' in text:
                garage = 1

            if 'fireplace' in text:
                fireplace = True

        return {
            'Floor':     floor,
            'Garage':    garage,
            'Fireplace': fireplace,
            'Building':  building,
            'Lake View': lake_view,
            'Tour Available': tour_available,
            'Unit URL':       unit_url
        }

    # ── Scrape ──────────────────────────────────────────────────────
    url     = "https://www.windsorcommunities.com/properties/legacy-at-windsor/floorplans/"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    print("Fetching apartment data...")

    response = requests.get(url, headers=headers)
    soup     = BeautifulSoup(response.text, 'html.parser')
    units    = soup.find_all('article', attrs={'data-spaces-unit': True})
    rows     = []

    print(f"Found {len(units)} units on page")

    for unit in units:
        spaces_id = unit.get('data-spaces-id')
        unit_num  = unit.get('data-spaces-unit')
        unit_tour = unit.get('data-spaces-tooltip="360° Tour"')
        details   = get_more_details(unit_num, spaces_id, url)
        rows.append({
            'Date':           str(date.today()),
            'Unit Number':    str(unit_num).zfill(4),
            'Price':          unit.get('data-spaces-sort-price'),
            'Building':       details.get('Building'),
            'Floor Plan':     unit.get('data-spaces-sort-plan-name'),
            'SqFt':           unit.get('data-spaces-sort-area'),
            'Bedrooms':       unit.get('data-spaces-sort-bed'),
            'Bathrooms':      unit.get('data-spaces-sort-bath'),
            'Floor':          details.get('Floor'),
            'Garage':         details.get('Garage'),
            'Fireplace':      details.get('Fireplace'),
            'Lake View':      details.get('Lake View'),
            'Tour Available': details.get('Tour Available'),
            'Unit URL':       details.get('Unit URL')
        })
    print(f"Fetched {len(rows)} units\n")

    # ── Load existing CSVs (or create empty ones) ───────────────────
    price_path   = 'data/price_history.csv'
    details_path = 'data/unit_details.csv'

    os.makedirs('data', exist_ok=True)  # create data/ folder if it doesn't exist

    if os.path.exists(price_path):
        price_df = pd.read_csv(price_path, dtype={'Unit Number': str})
    else:
        price_df = pd.DataFrame(columns=['Date', 'Unit Number', 'Price'])

    if os.path.exists(details_path):
        details_df = pd.read_csv(details_path, dtype={
            'Unit Number':    str,
            'Unit Number':    str,
            'Floor Plan':     str,
            'Building':       str,
            'SqFt':           str,
            'Bedrooms':       str,
            'Bathrooms':      str,
            'Floor':          str,
            'Garage':         str,
            'Fireplace':      str,
            'Lake View':      str,
            'Price Max':      str,
            'Price Max Date': str,
            'Price Min':      str,
            'Price Min Date': str,
            'Last Available': str,
            'Tour Available': str,
            'Unit URL':       str,
            'Tour URL':       str,
        })
    else:
        details_df = pd.DataFrame(columns=[
            'Unit Number', 'Floor Plan', 'Building', 'SqFt', 'Bedrooms',
            'Bathrooms', 'Floor', 'Garage', 'Fireplace', 'Lake View',
            'Max Price', 'Max Price Date', 'Min Price', 'Min Price Date',
            'Last Available'])
    # ── Update Price History ─────────────────────────────────────────
    print("Updating Price History...")
    existing_prices = set(zip(price_df['Date'], price_df['Unit Number']))
    new_prices      = [r for r in rows
                   if (r['Date'], r['Unit Number']) not in existing_prices]

    if new_prices:
        new_prices_df = pd.DataFrame(new_prices)[['Date', 'Unit Number', 'Price']]
        price_df      = pd.concat([price_df, new_prices_df], ignore_index=True)
        price_df.to_csv(price_path, index=False)
        print(f"Added {len(new_prices)} new price entries\n")
    else:
        print("No new prices to add today\n")

    # ── Update Unit Details with Scrape Data (upsert) ────────────────
    print("Updating Unit Details...")
    new_units = []
    for row in rows:
        mask = details_df['Unit Number'] == row['Unit Number']
        if mask.any():
            for col in ['Floor Plan', 'Building', 'SqFt', 'Bedrooms',
                        'Bathrooms', 'Floor', 'Garage', 'Fireplace', 
                        'Lake View', 'Tour Available', 'Unit URL']:
                details_df.loc[mask, col] = str(row[col])
        else:
            details_df = pd.concat([details_df, pd.DataFrame([{
                'Unit Number':    row['Unit Number'],
                'Floor Plan':     row['Floor Plan'],
                'Building':       row['Building'],
                'SqFt':           row['SqFt'],
                'Bedrooms':       row['Bedrooms'],
                'Bathrooms':      row['Bathrooms'],
                'Floor':          row['Floor'],
                'Garage':         row['Garage'],
                'Fireplace':      row['Fireplace'],
                'Lake View':      row['Lake View'],
                'Tour Available': row['Tour Available'],
                'Tour URL':       None,
                'Unit URL':       row['Unit URL'],
                'Price Max':      None,
                'Price Max Date': None,
                'Price Min':      None,
                'Price Min Date': None,
                'Last Available': None
            }])], ignore_index=True)
            new_units.append(row['Unit Number'])

    details_df = details_df.sort_values('Unit Number')
    details_df.to_csv(details_path, index=False)
    print(f"Upserted {len(rows)} units, {len(new_units)} new\n")

    # ── Update Max/Min ───────────────────────────────────────────────
    print("Updating max/mins")
    price_df['Price'] = pd.to_numeric(price_df['Price'], errors='coerce')
    price_df['Date']  = pd.to_datetime(price_df['Date'])
    today             = pd.Timestamp(date.today())

    new_max_mins = []
    
    for idx, unit_row in details_df.iterrows():
        unit_prices = price_df[price_df['Unit Number'] == unit_row['Unit Number']]
        if unit_prices.empty:
            continue

        high_row = unit_prices.loc[unit_prices['Price'].idxmax()]
        low_row  = unit_prices.loc[unit_prices['Price'].idxmin()]

        if high_row['Date'] == today:
            new_max_mins.append({
                'Unit Number': high_row['Unit Number'],
                'Type':        'High',
                'Price':       high_row['Price']
            })
        elif low_row['Date'] == today:
            new_max_mins.append({
                'Unit Number': low_row['Unit Number'],
                'Type':        'Low',
                'Price':       low_row['Price']
            })

        details_df.at[idx, 'Price Max']      = str(high_row['Price'])
        details_df.at[idx, 'Price Max Date'] = high_row['Date'].strftime('%Y-%m-%d')
        details_df.at[idx, 'Price Min']      = str(low_row['Price'])
        details_df.at[idx, 'Price Min Date'] = low_row['Date'].strftime('%Y-%m-%d')
        details_df.at[idx, 'Last Available'] = unit_prices['Date'].max().strftime('%Y-%m-%d')
    
    details_df = details_df.sort_values('Unit Number')
    details_df.to_csv(details_path, index=False)
    print("Finished updating max/mins\n")

    # ── Update Webpage ──────────────────────────────────────────────────────
    print("Publishing data")
    os.makedirs('docs', exist_ok=True)
    shutil.copy('data/price_history.csv', 'docs/price_history.csv')
    shutil.copy('data/unit_details.csv',  'docs/unit_details.csv')
    print("✅ Published data to docs\n")
    
    # ── Summary ──────────────────────────────────────────────────────
    print("------------------------")
    print(f"✅ Done! {len(rows)} units available on {date.today()}\n")
    if new_units:
        print("New units:", new_units, "")
    else:
        print("No new units today")

    if new_max_mins:
        print(f"\n{'Unit':<8} {'Type':<10} {'Price':>8}")
        print(f"{'─'*8} {'─'*10} {'─'*8}")
        for entry in new_max_mins:
            print(f"{entry['Unit Number']:<8} {entry['Type']:<10} ${entry['Price']:>7,.0f}")
    else:
        print("No new price highs or lows today")
    print("\n")

except Exception as e:
    print(f"❌ Error: {e}")
    traceback.print_exc()