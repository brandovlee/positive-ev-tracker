# Positive EV Tracker

## Overview
A monitoring tool that detects profitable betting opportunities by comparing real-time odds across multiple sportsbooks. It helps identify positive expected value (EV) bets where the bettor has an edge over the sportsbook.

## Purpose
Sportsbooks invest millions into engineers, actuaries, and statisticians to fine-tune their betting lines. So why not put them against each other?
Say you have $10 to wager and believe LeBron James will score over 20.5 points. If most sportsbooks offer a $20 payout but one offers $25, that discrepancy signals a potential positive EV opportunity.

## Example Output
<img width="583" height="263" alt="positive-ev-tracker" src="https://github.com/user-attachments/assets/ed4ed945-a2bb-4fba-96a1-137e22a4a424" />

## System Design

### `web-scrapers`
Collects lines and payout data from sportsbooks (DraftKings, PrizePicks, Underdog, Sleeper, ParlayPlay, Bet365).

### `main.py`
Compares lines and payouts across sportsbooks and sends a Discord webhook alert when a profitable discrepancy is found.
