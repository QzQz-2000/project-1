from fastapi import FastAPI, HTTPException
from app.models import FunctionRequest
from app.registry import FUNCTION_REGISTRY
import pandas as pd

app = FastAPI()

@app.post("/run-function")
def run_function(req: FunctionRequest):
    if req.function_name not in FUNCTION_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Function '{req.function_name}' not found.")

    try:
        fn_cls = FUNCTION_REGISTRY[req.function_name]
        fn = fn_cls()
        df_input = pd.DataFrame(req.data)
        df_output = fn.run(df_input, req.config)
        return {"result": df_output.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

