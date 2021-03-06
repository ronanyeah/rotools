'use strict'

const test = require('tape')
const exec = require(`${ROOT}/src/exec.js`)

test('exec', t => (
  t.plan(2),

  exec('pwd')
  .fork(
    t.fail,
    output =>
      t.ok(output.match('rotools'), 'exec pwd ok')
  ),

  exec('---------')
  .fork(
    err =>
      t.ok(err, 'exec fail ok'),
    t.fail
  )

) )
