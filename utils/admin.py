import json

from django.templatetags.static import static
from django.utils.html import format_html
from django.utils.safestring import mark_safe

from blockchains.models import Network


class EnableDisableAdminMixin:
    def enable(self, request, queryset):
        updated = queryset.update(is_enabled=True)
        self.message_user(request, f"{updated} items enabled.")

    enable.short_description = "Enable selected"

    def disable(self, request, queryset):
        updated = queryset.update(is_enabled=False)
        self.message_user(request, f"{updated} items disabled.")

    disable.short_description = "Disable selected"

    actions = ["enable", "disable"]


def format_pretty_json(json_data, highlight_keys=None):
    """
    Format JSON data with pretty styling and optional highlighting of specific keys.

    :param json_data: The JSON data to format.
    :param highlight_keys: Optional set of keys in dotted format to highlight.
    :return: Formatted and styled HTML-safe JSON data.
    """
    try:
        try:
            json_object = json.loads(json_data)
        except Exception:
            json_object = json_data

        formatted_data = json.dumps(json_object, indent=4, sort_keys=True)
        styled_json = style_json_keys_and_values(formatted_data, json_object, highlight_keys or set())
    except TypeError:
        return json_data
    return mark_safe(f'<pre>{styled_json}</pre>')


def style_json_keys_and_values(formatted_json, json_object, highlight_keys):
    """
    Style JSON keys and values.

    :param formatted_json: The formatted JSON string.
    :param json_object: The original JSON object.
    :param highlight_keys: Set of keys in dotted format to highlight.
    :return: Styled JSON string.
    """
    lines = formatted_json.splitlines()
    styled_lines = []

    for line in lines:
        styled_line = line
        if '": ' in line:  # This assumes typical "key": "value" structure
            key, value = line.split(': ', 1)
            styled_key = f'<span style="color: blue;">{key}</span>'
            styled_value = f'<span style="color: red;">{value}</span>'
            styled_line = f'{styled_key}: {styled_value}'
        styled_lines.append(styled_line)
    return '\n'.join(styled_lines)


def get_explorer_address_url(network: Network, address_id: str):
    if not address_id:
        return ""

    short_id = address_id[:8] + "...." + address_id[-8:]

    copy_icon_url = static('icons/copy_icon.png')
    copy_button = (
        f'<button onclick="navigator.clipboard.writeText(\'{address_id}\');'
        f'this.innerHTML=\'Copied!\';'
        f'setTimeout(() => this.innerHTML=\'<img src=\\\'{copy_icon_url}\\\' />\', 1000);'
        f'return false;" '
        f'style="border:none;background:none;cursor:pointer;padding:0 4px;">'
        f'<img src=\'{copy_icon_url}\' />'
        f'</button>'
    )

    if "ethereum" in network.name:
        return format_html(
            f"<a href='https://etherscan.io/address/{address_id}' target='_blank'>{short_id}</a>{copy_button}"
        )
    elif "polygon" in network.name:
        return format_html(
            f"<a href='https://polygonscan.com/address/{address_id}' target='_blank'>{short_id}</a>{copy_button}"
        )
    elif "avalanche" in network.name:
        return format_html(
            f"<a href='https://snowtrace.io/address/{address_id}' target='_blank'>{short_id}</a>{copy_button}"
        )
    elif "tron" in network.name:
        return format_html(
            f"<a href='https://tronscan.org/#/address/{address_id}' target='_blank'>{short_id}</a>{copy_button}"
        )
    elif "arbitrum" in network.name:
        return format_html(
            f"<a href='https://arbiscan.io/address/{address_id}' target='_blank'>{short_id}</a>{copy_button}"
        )
    else:
        return "Configure explorer URLs"


def get_explorer_transaction_url(network: Network, tx: str):
    if not tx:
        return ""

    short_hash = tx[:8] + "...." + tx[-8:]

    copy_icon_url = static('icons/copy_icon.png')
    copy_button = (
        f'<button onclick="navigator.clipboard.writeText(\'{tx}\');'
        f'this.innerHTML=\'Copied!\';'
        f'setTimeout(() => this.innerHTML=\'<img src=\\\'{copy_icon_url}\\\' />\', 1000);'
        f'return false;" '
        f'style="border:none;background:none;cursor:pointer;padding:0 4px;">'
        f'<img src=\'{copy_icon_url}\' />'
        f'</button>'
    )

    if "ethereum" in network.name:
        return format_html(
            f"<a href='https://etherscan.io/tx/{tx}' target='_blank'>{short_hash}</a>{copy_button}"
        )
    elif "polygon" in network.name:
        return format_html(
            f"<a href='https://polygonscan.com/tx/{tx}' target='_blank'>{short_hash}</a>{copy_button}"
        )
    elif "avalanche" in network.name:
        return format_html(
            f"<a href='https://snowtrace.io/tx/{tx}' target='_blank'>{short_hash}</a>{copy_button}"
        )
    elif "tron" in network.name:
        return format_html(
            f"<a href='https://tronscan.org/#/transaction/{tx}' target='_blank'>{short_hash}</a>{copy_button}"
        )
    elif "arbitrum" in network.name:
        return format_html(
            f"<a href='https://arbiscan.io/tx/{tx}' target='_blank'>{short_hash}</a>{copy_button}"
        )
    else:
        return "Configure explorer URLs"
