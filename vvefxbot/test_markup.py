from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

_SKIP_REASONS = [
    "Spread High",
    "News Window",
    "Weak Displacement",
    "Fake Sweep",
    "Late Entry",
    "Bad Session",
    "Structure Unclear",
    "Manual Reject",
]

signal_id = "test_sig_001"
markup = InlineKeyboardMarkup(row_width=2)
buttons = [
    InlineKeyboardButton(reason, callback_data=f"REASON_{signal_id}_{reason}")
    for reason in _SKIP_REASONS
]
buttons.append(InlineKeyboardButton("✍️ Manual Reason", callback_data=f"MANUAL_REASON_{signal_id}"))
markup.add(*buttons)

print(markup.to_dict())
