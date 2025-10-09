from flask import Flask, render_template, request
import pandas as pd
from montecarlo import monte_carlo_projection

app = Flask(__name__)

# Load fund info once (names & IDs)
funds_df = pd.read_csv("fund_allocation.csv")[["FUND_ID","FUND_NAME"]].drop_duplicates()

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/monte-carlo", methods=["GET", "POST"])
def monte_carlo():
    selected_fund = None
    amount = None
    results = None
    error = None

    if request.method == "POST":
        selected_fund = request.form.get("fund")
        amount_raw = request.form.get("amount", "")
        try:
            amount = float(amount_raw)
            if amount <= 0:
                raise ValueError("Amount must be greater than 0.")
        except Exception as e:
            error = f"Invalid amount: {e}"
            amount = None

        if not error and selected_fund:
            try:
                # ensure fund id is int for montecarlo function
                results = monte_carlo_projection(int(selected_fund), initial=amount, n_sims=3000)
            except Exception as e:
                # Catch errors (e.g. CSV not found, bad columns, etc.) and show a friendly message
                error = f"Simulation error: {e}"
                # Also print stacktrace to console for debugging
                import traceback
                traceback.print_exc()

    return render_template(
        "monte-carlo.html",
        funds=funds_df.to_dict("records"),
        selected_fund=str(selected_fund) if selected_fund is not None else None,
        amount=amount,
        results=results,
        error=error
    )

if __name__ == "__main__":
    app.run(debug=True)
