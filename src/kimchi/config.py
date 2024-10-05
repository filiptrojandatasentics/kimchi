score_lb = -50
score_ub = +50
decay_per_day = 5.0  # changed from suggested 10/7 (10 pts every week)

session_delay_rule = [
    (0.00, 0.80, +5.0),
    (0.80, 1.20,  0.0),
    (1.20, 1.35, -5.0),
    (1.35, 1.50, -7.5),
    (1.50, float("+inf"), -10.0),
]

main_signals = {
    "session_started": 20,
    "securities_account_overview_displayed": 15,
    "portfolio_overview_displayed": 15,
    "product_selection_confirmed": 7.5,
    "securities_orders_button_selected": 7.5,
    "watchlist_menu_button_selected": 7.5,
    "individual_recategorization_displayed": 5,
    "bulk_recategorization_triggered": 5,
    "individual_recategorization_triggered": 5,
    "feedback_selected": 5,
    "inbox_opened": 3,
}

minor_signals_list = [
    "dashboard_item_visibility_changed",
    "my_spendings_detail_displayed",
    "spotlight_detail_displayed",
    "insight_presented_on_overview",
    "instant_cash_offer_selected",
    "spending_budget_screen_displayed",
    "bulk_recategorization_displayed",
    "interval_changed",
    "insight_open"
    "local_use_case_selected",
    "product_selection_displayed"
    "spendings_tab_switched",
    "transaction_screen_interval_changed",
    "merchant_transactions_screen_displayed",
    "bulk_recategorization_triggered",
    "info_button_selected",
    "statements_button_selected",
    "store_landing_page_displayed",
]

minor_signals = {k: 2.0 for k in minor_signals_list}
signals = main_signals | minor_signals
