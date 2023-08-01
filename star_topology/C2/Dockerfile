# Use ASP.NET runtime image
FROM mcr.microsoft.com/dotnet/aspnet:7.0
WORKDIR /app

# Copy publish output from local machine
COPY ./star_topology/C2/out .
COPY ./star_topology/C2/faulty_consumer.json /app/faulty_consumer.json

ENTRYPOINT ["dotnet", "Consumer.dll"]
