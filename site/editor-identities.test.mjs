import assert from "node:assert/strict";
import {test} from "node:test";
import metadata from "../config/editor-identity-transitions.json" with {type: "json"};
import {identityTransition} from "./src/components/editor-identities.js";

test("temporary-account transition maps to the selected chart period", () => {
  assert.equal(identityTransition(metadata, "frwiki", "month").period, "2025-07");
  assert.equal(identityTransition(metadata, "frwiki", "quarter").period, "2025-Q3");
  assert.equal(identityTransition(metadata, "frwiki", "year").period, "2025");
  assert.equal(identityTransition(metadata, "nlwiki", "month"), null);
});
