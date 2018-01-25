#! /usr/bin/python3

"""Pay out dividends."""
import json
import struct
import decimal
D = decimal.Decimal
import logging
logger = logging.getLogger(__name__)

from counterpartylib.lib import (config, exceptions, util, message_type)

FORMAT_1 = '>QQ'
LENGTH_1 = 8 + 8
FORMAT_2 = '>QQQ'
LENGTH_2 = 8 + 8 + 8
ID = 50

def initialise (db):
    cursor = db.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS dividends(
                      tx_index INTEGER PRIMARY KEY,
                      tx_hash TEXT UNIQUE,
                      block_index INTEGER,
                      source TEXT,
                      asset TEXT,
                      dividend_asset TEXT,
                      quantity_per_unit INTEGER,
                      fee_paid INTEGER,
                      status TEXT,
                      FOREIGN KEY (tx_index, tx_hash, block_index) REFERENCES transactions(tx_index, tx_hash, block_index))
                   ''')
    cursor.execute('''CREATE INDEX IF NOT EXISTS
                      block_index_idx ON dividends (block_index)
                   ''')
    cursor.execute('''CREATE INDEX IF NOT EXISTS
                      source_idx ON dividends (source)
                   ''')
    cursor.execute('''CREATE INDEX IF NOT EXISTS
                      asset_idx ON dividends (asset)
                   ''')

    if util.enabled('scheduled_dividends'):
        # Add activate_index to dividends
        #   SQLite can’t do `ALTER TABLE IF COLUMN NOT EXISTS`.
        columns = [column['name'] for column in cursor.execute('''PRAGMA table_info(dividends)''')]
        if 'activate_index' not in columns:
            cursor.execute('''ALTER TABLE dividends ADD COLUMN activate_index INTEGER DEFAULT 0''')

        cursor.execute('''CREATE INDEX IF NOT EXISTS
                          activate_idx ON dividends (activate_index, status)
                       ''')

        # Dividend Activations
        cursor.execute('''CREATE TABLE IF NOT EXISTS dividend_activations(
                          dividend_index INTEGER PRIMARY KEY,
                          dividend_hash TEXT UNIQUE,
                          source TEXT,
                          block_index INTEGER,
                          FOREIGN KEY (block_index) REFERENCES blocks(block_index),
                          FOREIGN KEY (dividend_index, dividend_hash) REFERENCES dividends(tx_index, tx_hash))
                       ''')
        cursor.execute('''CREATE INDEX IF NOT EXISTS
                          block_index_idx ON dividend_activations (block_index)
                       ''')
        cursor.execute('''CREATE INDEX IF NOT EXISTS
                          source_idx ON dividend_activations (source)
                       ''')


def activate_dividend (db, dividend, status, block_index):
    cursor = db.cursor()

    # Update status of dividend.
    bindings = {
        'status': status,
        'tx_hash': dividend['tx_hash']
    }
    sql='update dividends set status = :status where tx_hash = :tx_hash'
    cursor.execute(sql, bindings)
    log.message(db, block_index, 'update', 'dividends', bindings)

    if dividend['dividend_asset'] != config.BTC:    # Can’t credit BTC.
        holders = util.holders(db, dividend['asset'])

        for holder in holders:
            address = holder['address']
            address_quantity = holder['address_quantity']
            if block_index >= 296000 or config.TESTNET: # Protocol change.
                if address == source: continue

            dividend_quantity = address_quantity * quantity_per_unit
            if divisible: dividend_quantity /= config.UNIT
            if not dividend_divisible: dividend_quantity /= config.UNIT
            if dividend_asset == config.BTC and dividend_quantity < config.DEFAULT_MULTISIG_DUST_SIZE: continue    # A bit hackish.
            dividend_quantity = int(dividend_quantity)

            util.credit(db, address, dividend['dividend_asset'], dividend_quantity, action='dividend', event=tx['tx_hash'])

    if status == 'complete':
        # Record dividend activation.
        bindings = {
            'dividend_index': dividend['tx_index'],
            'dividend_hash': dividend['tx_hash'],
            'source': dividend['source'],
            'block_index': block_index
        }
        sql='insert into dividend_activations values(:dividend_index, :dividend_hash, :source, :block_index)'
        cursor.execute(sql, bindings)

    cursor.close()

def validate (db, source, quantity_per_unit, asset, dividend_asset, block_index, activation=None):
    cursor = db.cursor()
    problems = []

    if asset == config.BTC:
        problems.append('cannot pay dividends to holders of {}'.format(config.BTC))
    if asset == config.XCP:
        if (not block_index >= 317500) or block_index >= 320000 or config.TESTNET:   # Protocol change.
            problems.append('cannot pay dividends to holders of {}'.format(config.XCP))

    if quantity_per_unit <= 0:
        problems.append('non‐positive quantity per unit')

    # For SQLite3
    if quantity_per_unit > config.MAX_INT:
        problems.append('integer overflow')

    # Enforce activation rules
    if(activation is not None):
        if util.enabled('scheduled_dividends'):
            if(activation < 0): problems.append('negative activation')
            if not isinstance(activation, int):
                problems.append('activation must be expressed as an integer block delta')
            if(activation <= block_index):
                problems.append('activation must be future block height')
            if(activation - block_index < 144):
                problems.append('activation must be 144 or more blocks from now')
            if activation > config.MAX_ACTIVATION:
                problems.append('activation overflow')
        else:
                problems.append('scheduled dividends are not enabled')

    # Examine asset.
    issuances = list(cursor.execute('''SELECT * FROM issuances WHERE (status = ? AND asset = ?) ORDER BY tx_index ASC''', ('valid', asset)))
    if not issuances:
        problems.append('no such asset, {}.'.format(asset))
        return None, None, problems, 0
    divisible = issuances[0]['divisible']

    # Only issuer can pay dividends.
    if block_index >= 320000 or config.TESTNET:   # Protocol change.
        if issuances[-1]['issuer'] != source:
            problems.append('only issuer can pay dividends')

    # Enforce locked requirement
    if(activation is not None):
        if(not issuances[-1]['locked']):
            problems.append('asset and dividend_asset must be locked')

    # Examine dividend asset.
    if dividend_asset in (config.BTC, config.XCP):
        dividend_divisible = True
    else:
        issuances = list(cursor.execute('''SELECT * FROM issuances WHERE (status = ? AND asset = ?)''', ('valid', dividend_asset)))
        if not issuances:
            problems.append('no such dividend asset, {}.'.format(dividend_asset))
            return None, None, problems, 0
        dividend_divisible = issuances[0]['divisible']

    # Enforce locked requirement
    if(activation is not None):
        if(not issuances[-1]['locked']):
            problems.append('asset and dividend_asset must be locked')

    # Calculate dividend quantities.
    holders = util.holders(db, asset)
    outputs = []
    addresses = []
    dividend_total = 0

    if(activation is not None):
        issuances = list(cursor.execute('''SELECT * FROM issuances WHERE (status = ? AND asset = ?)''', ('valid', asset)))
        asset_issuance = sum([issuance['quantity'] for issuance in issuances])

        dividend_quantity = asset_issuance * quantity_per_unit
        if divisible: dividend_quantity /= config.UNIT
        if not dividend_divisible: dividend_quantity /= config.UNIT
        if dividend_asset == config.BTC and dividend_quantity < config.DEFAULT_MULTISIG_DUST_SIZE: continue    # A bit hackish.

        dividend_total = int(dividend_quantity)
    else:
        for holder in holders:

            if block_index < 294500 and not config.TESTNET: # Protocol change.
                if holder['escrow']: continue

            address = holder['address']
            address_quantity = holder['address_quantity']
            if block_index >= 296000 or config.TESTNET: # Protocol change.
                if address == source: continue

            dividend_quantity = address_quantity * quantity_per_unit
            if divisible: dividend_quantity /= config.UNIT
            if not dividend_divisible: dividend_quantity /= config.UNIT
            if dividend_asset == config.BTC and dividend_quantity < config.DEFAULT_MULTISIG_DUST_SIZE: continue    # A bit hackish.
            dividend_quantity = int(dividend_quantity)

            outputs.append({'address': address, 'address_quantity': address_quantity, 'dividend_quantity': dividend_quantity})
            addresses.append(address)
            dividend_total += dividend_quantity

    if not dividend_total: problems.append('zero dividend')

    if dividend_asset != config.BTC:
        dividend_balances = list(cursor.execute('''SELECT * FROM balances WHERE (address = ? AND asset = ?)''', (source, dividend_asset)))
        if not dividend_balances or dividend_balances[0]['quantity'] < dividend_total:
            problems.append('insufficient funds ({})'.format(dividend_asset))

    fee = 0
    if not problems and dividend_asset != config.BTC:
        holder_count = len(set(addresses))
        if block_index >= 330000 or config.TESTNET: # Protocol change.
            if(activation is not None):
                fee = int(0.02 * config.UNIT)
            else:
                fee = int(0.0002 * config.UNIT * holder_count)
        if fee:
            balances = list(cursor.execute('''SELECT * FROM balances WHERE (address = ? AND asset = ?)''', (source, config.XCP)))
            if not balances or balances[0]['quantity'] < fee:
                problems.append('insufficient funds ({})'.format(config.XCP))

    if not problems and dividend_asset == config.XCP:
        total_cost = dividend_total + fee
        if not dividend_balances or dividend_balances[0]['quantity'] < total_cost:
            problems.append('insufficient funds ({})'.format(dividend_asset))

    # For SQLite3
    if fee > config.MAX_INT or dividend_total > config.MAX_INT:
        problems.append('integer overflow')

    cursor.close()
    return dividend_total, outputs, problems, fee

def compose (db, source, quantity_per_unit, asset, dividend_asset, activation=None):
    # resolve subassets
    asset = util.resolve_subasset_longname(db, asset)
    dividend_asset = util.resolve_subasset_longname(db, dividend_asset)

    dividend_total, outputs, problems, fee = validate(db, source, quantity_per_unit, asset, dividend_asset, util.CURRENT_BLOCK_INDEX)
    if problems: raise exceptions.ComposeError(problems)
    logger.info('Total quantity to be distributed in dividends: {} {}'.format(util.value_out(db, dividend_total, dividend_asset), dividend_asset))

    if dividend_asset == config.BTC:
        return (source, [(output['address'], output['dividend_quantity']) for output in outputs], None)

    asset_id = util.get_asset_id(db, asset, util.CURRENT_BLOCK_INDEX)
    dividend_asset_id = util.get_asset_id(db, dividend_asset, util.CURRENT_BLOCK_INDEX)
    data = message_type.pack(ID)
    data += struct.pack(FORMAT_2, quantity_per_unit, asset_id, dividend_asset_id, activation)
    return (source, [], data)

def parse (db, tx, message):
    dividend_parse_cursor = db.cursor()

    # Unpack message.
    try:
        if (tx['block_index'] > 288150 or config.TESTNET) and len(message) == LENGTH_2:
            quantity_per_unit, asset_id, dividend_asset_id, activation = struct.unpack(FORMAT_2, message)
            asset = util.get_asset_name(db, asset_id, tx['block_index'])
            dividend_asset = util.get_asset_name(db, dividend_asset_id, tx['block_index'])
            status = 'valid' if activation is None else 'pending'
        elif len(message) == LENGTH_1:
            quantity_per_unit, asset_id = struct.unpack(FORMAT_1, message)
            asset = util.get_asset_name(db, asset_id, tx['block_index'])
            dividend_asset = config.XCP
            status = 'valid'
        else:
            raise exceptions.UnpackError
    except (exceptions.UnpackError, exceptions.AssetNameError, struct.error) as e:
        dividend_asset, quantity_per_unit, asset, activation = None, None, None, None
        status = 'invalid: could not unpack'

    if dividend_asset == config.BTC:
        status = 'invalid: cannot pay {} dividends within protocol'.format(config.BTC)

    if status == 'valid' or 'pending':
        # For SQLite3
        quantity_per_unit = min(quantity_per_unit, config.MAX_INT)

        dividend_total, outputs, problems, fee = validate(db, tx['source'], quantity_per_unit, asset, dividend_asset, block_index=tx['block_index'], activation)
        if problems: status = 'invalid: ' + '; '.join(problems)

    if status == 'valid':
        # Debit.
        util.debit(db, tx['source'], dividend_asset, dividend_total, action='dividend', event=tx['tx_hash'])
        if tx['block_index'] >= 330000 or config.TESTNET: # Protocol change.
            util.debit(db, tx['source'], config.XCP, fee, action='dividend fee', event=tx['tx_hash'])

        # Credit.
        for output in outputs:
            util.credit(db, output['address'], dividend_asset, output['dividend_quantity'], action='dividend', event=tx['tx_hash'])

    if status == 'pending':
        # Debit.
        util.debit(db, tx['source'], dividend_asset, dividend_total, action='pending dividend', event=tx['tx_hash'])
        if tx['block_index'] >= 330000 or config.TESTNET: # Protocol change.
            util.debit(db, tx['source'], config.XCP, fee, action='dividend fee', event=tx['tx_hash'])

    # Add parsed transaction to message-type–specific table.
    bindings = {
        'tx_index': tx['tx_index'],
        'tx_hash': tx['tx_hash'],
        'block_index': tx['block_index'],
        'source': tx['source'],
        'asset': asset,
        'dividend_asset': dividend_asset,
        'quantity_per_unit': quantity_per_unit,
        'fee_paid': fee,
        'status': status,
        'activation': activation,
    }

    if "integer overflow" not in status:
        sql = 'insert into dividends values(:tx_index, :tx_hash, :block_index, :source, :asset, :dividend_asset, :quantity_per_unit, :fee_paid, :status, :activation)'
        dividend_parse_cursor.execute(sql, bindings)
    else:
        logger.warn("Not storing [dividend] tx [%s]: %s" % (tx['tx_hash'], status))
        logger.debug("Bindings: %s" % (json.dumps(bindings), ))

    dividend_parse_cursor.close()

def activate (db, block_index):
    cursor = db.cursor()

    # Parse scheduled dividends
    cursor.execute('''SELECT * FROM dividends \
                      WHERE (status = ? AND activate_index < ?)''', ('pending', block_index))
    dividends = list(cursor)
    for dividends in dividend:
        activate_dividend(db, dividend, 'complete', block_index)

    cursor.close()

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
