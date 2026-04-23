import pandas as pd
import numpy as np
from itertools import combinations


class UniversalExtractor:

    def extract(self, df: pd.DataFrame):
        roles = self._infer_roles(df)
        return self._build_items(df, roles)

    def _infer_roles(self, df):
        numeric = [c for c in df.columns if self._is_numeric(df[c])]
        text    = [c for c in df.columns if c not in numeric]

        roles = {}

        # item column
        if text:
            roles["item"] = max(text, key=lambda c: df[c].astype(str).str.len().mean())

        best = None
        best_score = -1

        for a, b, c in combinations(numeric, 3):
            score = self._score_triplet(df[a], df[b], df[c])
            if score > best_score:
                best_score = score
                best = (a, b, c)

        if best:
            roles.update(self._assign_qpt(df, *best))

        return roles

    def _score_triplet(self, q, p, t):
        q = pd.to_numeric(q, errors='coerce')
        p = pd.to_numeric(p, errors='coerce')
        t = pd.to_numeric(t, errors='coerce')

        mask = (~q.isna()) & (~p.isna()) & (~t.isna())
        if mask.sum() < 3:
            return -999

        err = np.abs(q[mask] * p[mask] - t[mask])
        return -np.mean(err / (t[mask] + 1e-6))

    def _assign_qpt(self, df, a, b, c):
        cols = [a, b, c]
        means = {col: pd.to_numeric(df[col], errors='coerce').mean() for col in cols}

        total = max(means, key=means.get)
        others = [x for x in cols if x != total]

        if means[others[0]] > means[others[1]]:
            unit, qty = others[0], others[1]
        else:
            unit, qty = others[1], others[0]

        return {
            "quantity": qty,
            "unit_price": unit,
            "total_price": total
        }

    def _build_items(self, df, roles):
        items = []

        for _, r in df.iterrows():
            items.append({
                "name": str(r.get(roles.get("item", ""), "")),
                "quantity": self._to_float(r.get(roles.get("quantity"))),
                "unit_price": self._to_float(r.get(roles.get("unit_price"))),
                "total_price": self._to_float(r.get(roles.get("total_price")))
            })

        return items

    def _is_numeric(self, s):
        try:
            pd.to_numeric(s.dropna().astype(str).str.replace(",", "."))
            return True
        except:
            return False

    def _to_float(self, x):
        try:
            return float(str(x).replace(",", "."))
        except:
            return None