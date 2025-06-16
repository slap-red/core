# captcha_urls_android.py
# A simple script to bulk-open a list of URLs on Android using Termux.
# This list is populated with sites that failed due to an "Invalid Captcha" error.

import os
import time

# --- URL List for 'Invalid Captcha' Failures ---
urls_to_open = [
    "https://100pokies.com",
    "https://9aus.com",
    "https://afl88.com",
    "https://all117.com",
    "https://anzspin.com",
    "https://ariswin.com",
    "https://arkspin.com",
    "https://aus2u.com",
    "https://aus33.com",
    "https://ausbet88.com",
    "https://aussie21.com",
    "https://aussieluck33au.com",
    "https://ausking777.com",
    "https://auslot7.com",
    "https://avengers9.net",
    "https://bacca777.com",
    "https://bankau.live",
    "https://bet365aud.com",
    "https://betaaron.com",
    "https://betblaze.org",
    "https://betcody.com",
    "https://betjohn.net",
    "https://betman9.com",
    "https://betnich.com",
    "https://betoptus.com",
    "https://betus10.co",
    "https://betworld96au.com",
    "https://betzilla88.com",
    "https://bigpay77.net",
    "https://bizzo777.com",
    "https://blackpokies.com",
    "https://blaze007.com",
    "https://bm7au.com",
    "https://bmb99.com",
    "https://bn8aus.com",
    "https://bondi333.com",
    "https://bonsai369.com",
    "https://bonza7.com",
    "https://bonza96.com",
    "https://boombaby9.com",
    "https://boom966.com",
    "https://boss365au.com",
    "https://bountyspin.com",
    "https://bpay7.com",
    "https://breakspin.com",
    "https://breakwin.com",
    "https://brismelb6.co",
    "https://buffalo39a.com",
    "https://bunny96.com",
    "https://bybid9.com",
    "https://bx77au.com",
    "https://candy96.com",
    "https://cashking99.com",
    "https://cergas.online",
    "https://champion9.com",
    "https://checkmate7.com",
    "https://class777.com",
    "https://click96.com",
    "https://clownwin.com",
    "https://cocainespin.com",
    "https://cocspin.com",
    "https://cola88au.co",
    "https://coospin.com",
    "https://crown69.com",
    "https://crown69.co",
    "https://crown777au.com",
    "https://crownbet.pro",
    "https://crystalchips777.com",
    "https://cuntspin.com",
    "https://cuntwin.com",
    "https://cyberpunk369.com",
    "https://dd8au.com",
    "https://dnf33.com",
    "https://dogdog11.com",
    "https://dolphin88.co",
    "https://donaldwin.com",
    "https://dowin8aus.com",
    "https://drpokies.com",
    "https://dsyaus.com",
    "https://e99au.com",
    "https://ecstasybaby.com",
    "https://emax7.co",
    "https://emu668.co",
    "https://enjoy007.com",
    "https://enjoy2win99.com",
    "https://enjoy33.vip",
    "https://epicpokies.com",
    "https://eq9au.com",
    "https://everwin44.com",
    "https://ex77au.com",
    "https://extrawin9.com",
]
# --------------------------------------------------

# Main execution loop
for url in urls_to_open:
    # Construct the command using the termux-open utility
    command = f"termux-open '{url}'"
    
    print(f"Opening: {url}")
    
    # Execute the command
    os.system(command)
    
    # Wait for 2 seconds before opening the next link. Adjust if needed.
    time.sleep(2)

print("\n--- All links have been opened. ---")