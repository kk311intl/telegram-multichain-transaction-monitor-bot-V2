import json
import tempfile
import time
import unittest
from pathlib import Path
from dataclasses import replace
from decimal import Decimal
from unittest.mock import patch
from bot_runtime.security import TokenSecurity, SecurityDeferred
from bot_runtime.store import Store
from bot_runtime.common import Event
from bot_runtime.filters import ScamFilter
from bot_runtime.market import MarketAssessment


class SecurityTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.store=Store(Path(self.tmp.name)/'db',100)
        self.security=TokenSecurity()
        self.cfg=json.loads((Path(__file__).parent / "examples/chains.example.json").read_text(encoding='utf-8'))['chains']['ethereum']
        self.asset='0x'+'1'*40
        self.store.set_meta('goplus_supported',json.dumps({'ids':['1','tron'],'expires':time.time()+86400}))

    def tearDown(self):
        self.store.db.close()
        self.tmp.cleanup()

    def result(self,data):return {'code':1,'result':{self.asset:data}}

    def test_counterfeit_cache_is_shared_and_persistent(self):
        with patch('bot_runtime.security.get_json',return_value=self.result({'fake_token':{'value':1}})) as api:
            self.assertEqual(self.security.assess(self.store,self.cfg,self.asset),'goplus_fake_token')
            self.assertEqual(TokenSecurity().assess(self.store,self.cfg,self.asset),'goplus_fake_token')
            self.assertEqual(api.call_count,1)

    def test_expired_risk_survives_outage(self):
        self.store.set_meta('goplus_token:1:'+self.asset,json.dumps({'risk':'goplus_fake_token','expires':0}))
        with patch('bot_runtime.security.get_json',side_effect=TimeoutError()) as api:
            self.assertEqual(self.security.assess(self.store,self.cfg,self.asset),'goplus_fake_token')
            self.assertEqual(self.security.assess(self.store,self.cfg,self.asset),'goplus_fake_token')
            self.assertEqual(api.call_count,1)

    def test_rate_budget_survives_new_client_and_does_not_call_api(self):
        with patch('bot_runtime.security.get_json',return_value=self.result({'is_honeypot':'0'})) as api:
            self.security.assess(self.store,self.cfg,self.asset)
            with self.assertRaises(SecurityDeferred):TokenSecurity().assess(self.store,self.cfg,'0x'+'2'*40)
            self.assertEqual(api.call_count,1)

    def test_unsupported_empty_and_tron_case(self):
        with patch('bot_runtime.security.get_json') as api:
            self.assertEqual(self.security.assess(self.store,{'type':'evm','expected_chain_id':999},self.asset),'')
            api.assert_not_called()
        with patch('bot_runtime.security.get_json',return_value={'code':1,'result':{}}):
            self.assertEqual(self.security.assess(self.store,self.cfg,self.asset),'')
        self.store.set_meta('goplus_next_request',0)
        self.asset='TCaseSensitive'
        with patch('bot_runtime.security.get_json',return_value=self.result({'is_honeypot':'1'})):
            self.assertEqual(self.security.assess(self.store,{'type':'tron'},self.asset),'goplus_honeypot')

    def test_security_and_market_are_independent_no_stablecoin_exemption(self):
        event=Event('ethereum','tx',1,'hash','address','in',self.asset,'USDT',10**18,18,'other')
        filt=ScamFilter({})
        rich=MarketAssessment(True,Decimal(10),Decimal(100000))
        self.assertEqual(filt.evaluate(replace(event,source_risk='goplus_fake_token'),rich),(True,'goplus_fake_token'))
        self.assertEqual(filt.evaluate(event,MarketAssessment(False,reason='market_liquidity_below_minimum')),(True,'market_liquidity_below_minimum'))
        self.assertEqual(filt.evaluate(event,MarketAssessment(True,Decimal('0.1'))),(True,'token_value_below_usd_minimum'))
        self.assertEqual(filt.evaluate(event,rich),(False,''))

    def test_market_failure_is_marked_and_cached_price_filters_screenshot_dust(self):
        from types import SimpleNamespace
        from bot_runtime.enrichment import TokenEnrichment
        from bot_runtime.market import DexScreenerOracle
        enrichment=TokenEnrichment(self.store.path if hasattr(self.store,'path') else Path(self.tmp.name)/'db',100,self.security,{})
        adapter=SimpleNamespace(name='optimism',config={})
        try:
            self.store.set_meta(enrichment.key('optimism',self.asset),json.dumps({'complete':True,'next':time.time()+60,'risk':'','symbol':'USDC','decimals':6}))
            self.assertIsNotNone(enrichment.request(adapter,self.store,self.asset))
            missing=Event('optimism','tx',1,'hash','address','in',self.asset,'USDC',1,6,'other')
            self.assertEqual(ScamFilter({}).evaluate(missing,None),(False,'market_check_unavailable'))
            from bot_runtime.telegram_controller import TelegramControllerMixin
            self.assertEqual(TelegramControllerMixin._assessment_warning('market_check_unavailable|security_unknown'),'⚠️ 無法判定是否爲惡意/詐騙交易 注意甄別\n')
            self.assertEqual(TelegramControllerMixin._assessment_warning('market_check_unavailable'),'')
            self.assertEqual(TelegramControllerMixin._assessment_warning('security_check_unavailable'),'')
            self.assertEqual(TelegramControllerMixin._assessment_warning('security_check_unavailable|market_cache_stale'),'')
            self.store.save_token_market('optimism',self.asset,True,Decimal('1'),Decimal('100000'),'')
            self.assertIsNotNone(enrichment.request(adapter,self.store,self.asset))
            event=Event('optimism','tx',1,'hash','address','in',self.asset,'USDC',1,6,'other')
            self.assertEqual(ScamFilter({}).evaluate(event,enrichment.assessment(self.store,'optimism',self.asset)),(True,'token_value_below_usd_minimum'))
            self.assertEqual(ScamFilter({}).evaluate(replace(event,symbol='OP',amount_raw=79015,decimals=18),MarketAssessment(True,Decimal('0.1'),Decimal('100000'))),(True,'token_value_below_usd_minimum'))
            pairs=[{'chainId':'optimism','baseToken':{'address':self.asset},'priceUsd':'1','liquidity':{'usd':'100000'}}]
            with patch('bot_runtime.market.get_json',return_value=pairs) as get:
                self.assertTrue(DexScreenerOracle().assess({'market_chain':'optimism'},self.asset).valuable)
                self.assertIn('User-Agent',get.call_args.args[1])
        finally:enrichment.close()

    def test_token_price_is_shared_for_24_hours_and_refreshed_only_once(self):
        from types import SimpleNamespace
        from bot_runtime.enrichment import TokenEnrichment
        cache=TokenEnrichment(Path(self.tmp.name)/'db',100,SimpleNamespace(assess=lambda *a:''),{})
        market=MarketAssessment(True,Decimal('1.01'),Decimal('100000'))
        self.store.save_token_market('ethereum',self.asset,True,market.price_usd,market.liquidity_usd,'')
        try:
            with patch('bot_runtime.enrichment.build_adapter',return_value=SimpleNamespace()), \
                 patch('bot_runtime.enrichment.metadata',return_value=('TOKEN',6,True)), \
                 patch('bot_runtime.enrichment.DexScreenerOracle.assess',return_value=market) as api:
                cache.lookup('ethereum',self.cfg,self.asset)
                cache.lookup('ethereum',self.cfg,self.asset)
                api.assert_not_called()
                with self.store.db:
                    self.store.db.execute('UPDATE token_market_cache SET checked_at=?',(int(time.time())-86401,))
                cache.lookup('ethereum',self.cfg,self.asset)
                cache.lookup('ethereum',self.cfg,self.asset)
                self.assertEqual(api.call_count,1)
        finally:cache.close()

    def test_goplus_risk_short_circuits_dex_lookup(self):
        from types import SimpleNamespace
        from bot_runtime.enrichment import TokenEnrichment
        cache=TokenEnrichment(Path(self.tmp.name)/'db',100,SimpleNamespace(assess=lambda *a:'goplus_fake_token'),{})
        try:
            with patch('bot_runtime.enrichment.build_adapter',return_value=SimpleNamespace()), \
                 patch('bot_runtime.enrichment.metadata',return_value=('TOKEN',6,True)), \
                 patch('bot_runtime.enrichment.DexScreenerOracle.assess') as api:
                cache.lookup('ethereum',self.cfg,self.asset)
                api.assert_not_called()
                saved=json.loads(self.store.meta(cache.key('ethereum',self.asset)))
                self.assertEqual(saved['risk'],'goplus_fake_token')
                event=Event('ethereum','tx',1,'hash','address','in',self.asset,'TOKEN',100,6,'other',source_risk=saved['risk'])
                self.assertEqual(ScamFilter({}).evaluate(event,None),(True,'goplus_fake_token'))
        finally:cache.close()


if __name__=='__main__':unittest.main()
