# %%
# Imports #

import os
import subprocess
import sys

# append grandparent
if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import utils.config_utils  # noqa E402
from utils.display_tools import print_logger

# %%
# Functions #


def check_vpn_connected():
    try:
        # Check if OpenVPN 3 is running
        result_openvpn3 = subprocess.run(["pgrep", "openvpn3"], stdout=subprocess.PIPE)
        openvpn3_running = bool(result_openvpn3.stdout)

        # Check if OpenVPN 2 is running
        result_openvpn2 = subprocess.run(["pgrep", "openvpn"], stdout=subprocess.PIPE)
        openvpn2_running = bool(result_openvpn2.stdout)

        # Return True if either OpenVPN 3 or OpenVPN 2 is running
        return openvpn3_running or openvpn2_running
    except Exception:
        return False


def connect_vpn():
    home = os.path.expanduser("~")
    config_path = os.path.join(home, "hellofresh.ovpn")
    auth_path = os.path.join(home, "vpnauth.conf")

    print_logger(f"Connecting to VPN using openvpn3 with config: {config_path}")
    try:
        subprocess.run(["openvpn3", "session-start", "--config", config_path])
        print_logger("VPN connected using openvpn3")
        return
    except Exception as e:
        print_logger(f"Error connecting to VPN using openvpn3: {e}")

    print_logger(f"Connecting to VPN using openvpn with config: {config_path}")
    try:
        # openvpn --config /root/hellofresh.ovpn
        subprocess.run(
            ["openvpn", "--config", config_path, "--auth-user-pass", auth_path]
        )
        print_logger("VPN connected using openvpn")
        return
    except Exception as e:
        print_logger(f"Error connecting to VPN using openvpn: {e}")


if not check_vpn_connected():
    print_logger("VPN not connected. Connecting...")
    try:
        connect_vpn()
        print_logger("VPN connected.")
    except Exception as e:
        print_logger(f"Error connecting to VPN: {e}")
else:
    print_logger("VPN already connected.")


# %%
