/**
 * @fileoverview Custom ESLint rules for CBT project
 * @author CBT Team
 *
 * This module exports custom ESLint rules that enforce color usage standards
 * in the CBT project. All colors should be defined in src/index.css using
 * the two-tier color architecture (primitive scales + semantic tokens).
 */

module.exports = {
  rules: {
    'no-hardcoded-colors': require('./no-hardcoded-colors.cjs'),
    'no-primitive-color-scales': require('./no-primitive-color-scales.cjs'),
  },
};
