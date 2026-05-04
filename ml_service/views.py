from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status

from .services import embed_text


@api_view(["POST"])
def embed(request):
    text = request.data.get("text")

    if not text:
        return Response(
            {"error": "Text is required"},
            status=status.HTTP_400_BAD_REQUEST
        )

    try:
        embedding = embed_text(text)
        return Response({"embedding": embedding})

    except Exception as e:
        return Response(
            {"error": str(e)},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )