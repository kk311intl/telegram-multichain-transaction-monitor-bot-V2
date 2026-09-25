import unittest
from bot_runtime.settings import BotSettings


class SettingsTest(unittest.TestCase):
    def test_defaults_and_private_preferences(self):
        default=BotSettings.from_env({})
        self.assertEqual(default.timezone_minutes,0)
        self.assertEqual(default.notification_seconds,5)
        custom=BotSettings.from_env({'DISPLAY_TIMEZONE_OFFSET_MINUTES':'540','RECENT_CACHE_MIB':'2048',
                                    'ADDRESS_PENDING_LIMIT':'20','BOT_USAGE_NOTICE':'Private deployment'})
        self.assertEqual(custom.timezone_minutes,540)
        self.assertEqual(custom.cache_mib,2048)
        self.assertEqual(custom.usage_notice,'Private deployment')

    def test_invalid_config_cannot_disable_safety_bounds(self):
        for env in [{'TELEGRAM_GLOBAL_RPS':'21'},{'NOTIFICATION_INTERVAL_SECONDS':'0'},
                    {'MENU_TTL_SECONDS':'never'},{'ADDRESS_PENDING_LIMIT':'0'},
                    {'DISPLAY_TIMEZONE_OFFSET_MINUTES':'900'},{'RECENT_CACHE_MIB':'-1'},
                    {'BOT_TITLE':''},{'BOT_USAGE_NOTICE':'a'*301},
                    {'TOKEN_LOOKUP_WORKERS':'8','TOKEN_LOOKUP_MAX_PENDING':'2'}]:
            with self.subTest(env=env),self.assertRaises(ValueError):BotSettings.from_env(env)
